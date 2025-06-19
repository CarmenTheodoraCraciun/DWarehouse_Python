from fastapi import FastAPI, HTTPException, Query, Path
from contextlib import asynccontextmanager
from database import get_cassandra_session
from app_services import AssetService, DataIngestionService
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import AsyncIterator
from datetime import date
from fastapi.responses import HTMLResponse
import json
from repositories import TimeSeriesRepository

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    session, cluster = get_cassandra_session()
    app.state.session = session
    app.state.cluster = cluster
    app.state.asset_service = AssetService(app.state.session)
    app.state.data_ingestion_service = DataIngestionService(app.state.session)

    from initialize_data import initialize_required_data
    initialize_required_data(app.state.session)

    yield

    cluster.shutdown()

app = FastAPI(lifespan=lifespan)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Cod de inițializare la pornirea aplicației
    session, cluster = get_cassandra_session()
    app.state.session = session
    app.state.cluster = cluster
    app.state.asset_service = AssetService(app.state.session)
    app.state.data_ingestion_service = DataIngestionService(app.state.session)
    
    # Importul și inițializarea datelor necesare (ex: data sources, assets)
    # Asigură-te că `initialize_data` este un modul valid și că funcția este corectă
    from initialize_data import initialize_required_data
    initialize_required_data(app.state.session)
    
    yield  # Aici aplicația rulează
    
    # Cod de curățare la oprirea aplicației
    cluster.shutdown()

# Endpoint pentru crearea unui nou asset
@app.post("/assets/{symbol}", response_model=dict)
async def create_asset(symbol: str):
    try:
        return app.state.asset_service.create_asset(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pentru ingestia de date
@app.post("/ingest/{symbol}", response_model=dict)
async def ingest_data(
    symbol: str,
    # Setează data de start și end implicite pentru ingestie, formatate ca string
    start: str = Query(default=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')),
    end: str = Query(default=datetime.now().strftime('%Y-%m-%d'))
):
    try:
        result = app.state.data_ingestion_service.ingest_data(symbol, start, end)
        return {
            "status": "success",
            "symbol": symbol,
            "records_ingested": result.get("records_ingested", 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pentru obținerea datelor unui asset
@app.get("/assets/{asset_id}", response_model=list)
async def get_asset(asset_id: str = Path(..., title="ID-ul asset-ului")):
    try:
        # Presupunând că AssetService are un atribut `repository` cu metoda `find_all`
        return app.state.asset_service.repository.find_all(asset_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pentru sursele de date
@app.get("/data-sources/{data_source_id}", response_model=list)
async def get_data_source(data_source_id: str = Path(..., title="ID-ul sursei de date")):
    try:
        from repositories import DataSourceRepository
        repo = DataSourceRepository(app.state.session)
        return repo.find_all(data_source_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pentru datele de serie temporală
@app.get("/time-series/{asset_id}/{data_source_id}", response_model=list)
async def get_time_series_data(
    asset_id: str = Path(..., title="ID-ul asset-ului"),
    data_source_id: str = Path(..., title="ID-ul sursei de date"),
    # Parametrii de query opționali pentru filtrarea pe interval de date
    start_date: date = Query(None, title="Start date for filtering"),
    end_date: date = Query(None, title="End date for filtering"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, le=1000)
):
    try:
        # Validare interval temporal
        if start_date and end_date and start_date > end_date:
            raise HTTPException(
                status_code=400, 
                detail="start_date must be before end_date"
            )
            
        from repositories import TimeSeriesRepository
        repo = TimeSeriesRepository(app.state.session)
        
        # Setare valori implicite pentru intervalul de date dacă nu sunt specificate
        if not start_date:
            start_date = date.today() - timedelta(days=30)
        if not end_date:
            end_date = date.today()
        
        # Obținere date cu filtrare și paginare
        data = repo.find_latest_per_date(
            asset_id,
            data_source_id,
            start_date,
            end_date
        )
        return data[offset:offset+limit]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Agregări adăugate la sfârșitul fișierului `controllers.py`
@app.get("/aggregations/record-counts", response_model=list)
async def get_record_counts():
    """
    Returnează numărul de înregistrări per asset și an
    Exemplu răspuns: [{"asset_id": "IBM", "year": 2023, "count": 250}]
    """
    try:
        query = "SELECT asset_id, business_date_year AS year, cnt AS count FROM totals"
        rows = app.state.session.execute(query)
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/aggregations/avg-volume", response_model=list)
async def get_avg_volume():
    """
    Returnează volumul mediu tranzacționat per asset, an și lună
    Exemplu răspuns: [{"asset_id": "IBM", "year": 2023, "month": 5, "avg_volume": 15000.75}]
    """
    try:
        query = "SELECT asset_id, business_date_year AS year, business_date_month AS month, avg_volume FROM monthly_avg_volume"
        rows = app.state.session.execute(query)
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/aggregations/avg-volume/{asset_id}", response_model=list)
async def get_avg_volume_by_asset(asset_id: str):
    """
    Returnează volumul mediu tranzacționat pentru un anumit asset
    """
    try:
        query = "SELECT asset_id, business_date_year AS year, business_date_month AS month, avg_volume FROM monthly_avg_volume WHERE asset_id = %s"
        rows = app.state.session.execute(query, [asset_id])
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

load_dotenv()

@app.get("/api/dashboard/{asset_id}/years", response_model=list)
async def get_available_years(asset_id: str):
    try:
        query = """
            SELECT business_date_year 
            FROM time_series_data 
            WHERE asset_id = %s AND data_source_id = 'ALPHAVANTAGE' 
            ALLOW FILTERING
        """
        rows = app.state.session.execute(query, [asset_id])
        
        # Construim un set de ani unici — folosind accesare dict dacă rezultatul e dict
        years = {row["business_date_year"] for row in rows if "business_date_year" in row}
        return sorted(years)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pentru predicții
@app.get("/api/dashboard/{asset_id}/predictions")
async def get_predictions_data(asset_id: str):
    try:
        pred_query = "SELECT * FROM predictions WHERE asset_id = %s ORDER BY prediction_date ASC LIMIT 5"
        prediction_rows = list(app.state.session.execute(pred_query, [asset_id]))
        
        # Formatare timpi de predicție
        formatted_rows = []
        for p in prediction_rows:
            row = dict(p)
            if hasattr(p['prediction_time'], 'strftime'):
                row['formatted_time'] = p['prediction_time'].strftime('%Y-%m-%d %H:%M')
            else:
                row['formatted_time'] = str(p['prediction_time'])
            formatted_rows.append(row)        
        return formatted_rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Adăugăm un nou endpoint pentru datele efective (ultimele 5 zile)
@app.get("/api/dashboard/{asset_id}/actual-data")
async def get_actual_data(asset_id: str, year: int = None):
    try:
        ts_repo = TimeSeriesRepository(app.state.session)

        if year:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
        else:
            end_date = date.today()
            start_date = end_date - timedelta(days=5)

        actual_data = ts_repo.find_latest_per_date(
            asset_id, 'ALPHAVANTAGE', start_date, end_date
        )

        # Sortăm descrescător după dată și luăm ultimele 5
        actual_data_sorted = sorted(
            actual_data, key=lambda x: x['business_date'], reverse=True
        )[:5]

        return actual_data_sorted

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Actualizăm endpoint-ul principal pentru dashboard
@app.get("/dashboard/{asset_id}", response_class=HTMLResponse)
async def dashboard(asset_id: str):
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ro">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard Financiar - {asset_id}</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <style>
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }}
            
            body {{
                background-color: #f5f7fa;
                color: #333;
                line-height: 1.6;
                padding: 20px;
            }}
            
            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}
            
            header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 30px;
                padding-bottom: 15px;
                border-bottom: 1px solid #e0e6ed;
            }}
            
            .logo {{
                font-size: 24px;
                font-weight: 700;
                color: #2563eb;
            }}
            
            .card-container {{
                display: flex;
                flex-direction: column;
                gap: 20px;
                margin-bottom: 30px;
            }}
            
            .card {{
                background: white;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                padding: 25px;
                transition: transform 0.3s ease;
            }}
            
            .card:hover {{
                transform: translateY(-5px);
                box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
            }}
            
            .card-title {{
                display: flex;
                align-items: center;
                margin-bottom: 20px;
                color: #1e293b;
                font-size: 1.2rem;
            }}
            
            .card-title i {{
                margin-right: 10px;
                font-size: 1.4rem;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
            }}
            
            th, td {{
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #e2e8f0;
            }}
            
            th {{
                background-color: #f1f5f9;
                font-weight: 600;
            }}
            
            tbody tr:hover {{
                background-color: #f8fafc;
            }}
            
            .loader {{
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                padding: 30px;
            }}
            
            .spinner {{
                width: 40px;
                height: 40px;
                border: 4px solid rgba(0, 0, 0, 0.1);
                border-left-color: #2563eb;
                border-radius: 50%;
                animation: spin 1s linear infinite;
                margin-bottom: 15px;
            }}
            
            @keyframes spin {{
                to {{ transform: rotate(360deg); }}
            }}
            
            .error {{
                display: flex;
                align-items: center;
                padding: 15px;
                background-color: #fee2e2;
                border-radius: 8px;
                color: #b91c1c;
            }}
            
            .error i {{
                margin-right: 10px;
                font-size: 1.5rem;
            }}
            
            footer {{
                text-align: center;
                padding: 20px;
                color: #64748b;
                border-top: 1px solid #e2e8f0;
                margin-top: 30px;
            }}
            
            h1 {{
                margin-bottom: 20px;
                color: #1e293b;
            }}

            select {{
                padding: 10px 14px;
                border: 1px solid #cbd5e1;
                border-radius: 6px;
                background-color: white;
                color: #1e293b;
                font-size: 14px;
                outline: none;
                transition: border-color 0.3s ease;
                margin-left: 10px;
                margin-bottom: 15px;
            }}

            select:focus {{
                border-color: #2563eb;
                box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.2);
                background-color: #f8fafc;
                color: #111827;
                cursor: pointer;
            }}

            select:hover {{
                border-color: #60a5fa;
            }}

        </style>
        <script>
        const assetId = "{asset_id}";

        function showError(containerId, message) {{
            const container = document.getElementById(containerId);
            container.innerHTML = `
                <div class="error">
                    <i class="fas fa-exclamation-triangle"></i> ${{message}}
                </div>
            `;
        }}


        function showLoader(containerId) {{
            const container = document.getElementById(containerId);
            container.innerHTML = `
                <div class="loader">
                    <div class="spinner"></div>
                    <p>Se încarcă datele...</p>
                </div>
            `;
        }}

        async function loadAvailableYears() {{
            try {{
                const response = await fetch(`/api/dashboard/${{assetId}}/years`);
                if (!response.ok) throw new Error("Nu se pot încărca anii");
                const years = await response.json();

                const select = document.getElementById("year-select");
                years.forEach(year => {{
                    const option = document.createElement("option");
                    option.value = year;
                    option.textContent = year;
                    select.appendChild(option);
                }});
            }} catch (err) {{
                console.error("Eroare la încărcarea anilor:", err);
            }}
        }}

        async function loadActualDataByYear() {{
                const year = document.getElementById("year-select").value;
                showLoader("actual-data-table");

                try {{
                    const url = year
                        ? `/api/dashboard/${{assetId}}/actual-data?year=${{year}}`
                        : `/api/dashboard/${{assetId}}/actual-data`;
                    const response = await fetch(url);
                    if (!response.ok) throw new Error("Eroare server");
                    const data = await response.json();

                    let rows = '';
                    if (data.length > 0) {{
                        rows = data.map(d => {{
                            const epochDays = d.business_date.days_from_epoch;
                            const date = new Date(epochDays * 86400000);
                            const formattedDate = !isNaN(date.getTime())
                                ? date.toISOString().split("T")[0]
                                : "—";

                            return `
                                <tr>
                                    <td>${{formattedDate}}</td>
                                    <td>$${{parseFloat(d.data_values.close).toFixed(2)}}</td>
                                    <td>${{parseInt(d.data_values.volume)}}</td>
                                    <td>${{parseFloat(d.data_values.open).toFixed(2)}}</td>
                                    <td>${{parseFloat(d.data_values.high).toFixed(2)}}</td>
                                    <td>${{parseFloat(d.data_values.low).toFixed(2)}}</td>
                                </tr>
                            `;
                        }}).join('');
                    }} else {{
                        rows = '<tr><td colspan="6">Nu există date pentru anul selectat</td></tr>';
                    }}

                    const table = `
                        <table>
                            <thead>
                                <tr>
                                    <th>Dată</th>
                                    <th>Închidere</th>
                                    <th>Volum</th>
                                    <th>Deschidere</th>
                                    <th>Maxim</th>
                                    <th>Minim</th>
                                </tr>
                            </thead>
                            <tbody>${{rows}}</tbody>
                        </table>
                    `;

                    document.getElementById("actual-data-table").innerHTML = table;
                }} catch (error) {{
                    showError("actual-data-table", error.message);
                }}
            }}

            async function loadPredictions() {{
                try {{
                    const response = await fetch(`/api/dashboard/${{assetId}}/predictions`);
                    if (!response.ok) throw new Error("Eroare la încărcarea predicțiilor");
                    const data = await response.json();

                    let rows = '';
                    if (data.length > 0) {{
                        rows = data.map(p => `
                            <tr>
                                <td>${{p.formatted_time}}</td>
                                <td>$${{p.predicted_close.toFixed(2)}}</td>
                            </tr>
                        `).join('');
                    }} else {{
                        rows = '<tr><td colspan="2">Nu există predicții disponibile</td></tr>';
                    }}

                    const table = `
                        <h2 class="card-title"><i class="fas fa-chart-line"></i> Predicții</h2>
                        <table>
                            <thead>
                                <tr>
                                    <th>Timp</th>
                                    <th>Preț prezis</th>
                                </tr>
                            </thead>
                            <tbody>${{rows}}</tbody>
                        </table>
                    `;

                    document.getElementById("predictions-content").innerHTML = table;
                }} catch (error) {{
                    document.getElementById("predictions-content").innerHTML = `
                        <div class="error"><i class="fas fa-exclamation-triangle"></i> ${{error.message}}</div>
                    `;
                }}
            }}

            document.addEventListener("DOMContentLoaded", () => {{
                loadAvailableYears();
                loadPredictions();
                loadActualDataByYear(); // inițial, încarcă ultimele 5 zile
            }});
        </script>
`
    </head>
    <body>
        <div class="container">
            <header>
                <div class="logo">Dashboard Financiar</div>
                <h1>Analiză pentru {asset_id}</h1>
            </header>
            
            <div class="card-container">

                <div class="card" id="actual-data-content">
                    <div class="card" id="actual-data-content">
                        <h2 class="card-title"><i class="fas fa-database"></i> Date Efective</h2>
                        <label for="year-select">Alege anul:</label>
                        <select id="year-select" onchange="loadActualDataByYear()">
                            <option value="">An</option>
                        </select>
                        <div id="actual-data-table"></div>
                    </div>
                </div>
            
                <div class="card" id="predictions-content">
                    <div class="loader">
                        <div class="spinner"></div>
                        <p>Se încarcă predicțiile...</p>
                    </div>
                </div>
            </div>
            
            <footer>
                <p>Dashboard financiar {asset_id} &copy; {datetime.now().year} | Date actualizate în timp real</p>
            </footer>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)