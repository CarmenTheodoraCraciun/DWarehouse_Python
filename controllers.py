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

app = FastAPI(lifespan=lifespan)

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
    
# Endpoint pentru dashboard-ul principal cu grafic și predicții
@app.get("/dashboard/{asset_id}", response_class=HTMLResponse)
async def dashboard(asset_id: str):
    try:
        # Obține date istorice
        ts_repo = TimeSeriesRepository(app.state.session)
        historical = ts_repo.find_latest_per_date(
            asset_id, 
            'ALPHAVANTAGE', 
            date.today() - timedelta(days=365), # Extins la 1 an pentru a asigura date
            date.today()
        )
        print(f"Historical data for {asset_id}: {historical}") # Debug print
        
        # Obține predicții
        pred_query = "SELECT * FROM predictions WHERE asset_id = %s ORDER BY prediction_date ASC"
        prediction_rows = list(app.state.session.execute(pred_query, [asset_id]))
        print(f"Prediction rows for {asset_id}: {prediction_rows}") # Debug print
        
        # Pregătește date pentru grafic
        historical_dates = [str(h['business_date']) for h in historical]
        historical_prices = [float(h['data_values']['close']) for h in historical]
        
        prediction_dates = [str(p['prediction_date']) for p in prediction_rows]
        prediction_prices = [float(p['predicted_close']) for p in prediction_rows]
        
        # Formatare timpi de predicție pentru afișare în tabel
        prediction_times = [p['prediction_time'] for p in prediction_rows]
        formatted_prediction_times = [t.strftime('%Y-%m-%d %H:%M') if hasattr(t, 'strftime') else str(t) for t in prediction_times]
        
        # Generează datele JSON separat pentru a evita conflictele și a le pasa direct JS
        historical_data_json = json.dumps([
            {'x': d, 'y': p} for d, p in zip(historical_dates, historical_prices)
        ])
        
        prediction_data_json = json.dumps([
            {'x': d, 'y': p} for d, p in zip(prediction_dates, prediction_prices)
        ])
        
        # Crează pagină HTML cu Chart.js
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Dashboard {asset_id}</title>
        </head>
        <body>
            <div class="container">
                <h1>Dashboard pentru {asset_id}</h1>
                
                <div class="chart-container">
                    <canvas id="priceChart"></canvas>
                </div>
                
                <div class="predictions">
                    <h2>Predicții recente:</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Data</th>
                                <th>Preț prezis</th>
                                <th>Model</th>
                                <th>Timp predicție</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join(
                                f"<tr>"
                                f"<td>{p['prediction_date']}</td>"
                                f"<td>${float(p['predicted_close']):.2f}</td>"
                                f"<td>{p['model_name']}</td>"
                                f"<td>{formatted_time}</td>"
                                f"</tr>" 
                                for p, formatted_time in zip(prediction_rows, formatted_prediction_times)
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
            
            <script>
                // Adaugăm o funcție pentru a formata datele de predicție pentru Chart.js
                // Presupunem că prediction_date vine ca string 'Date(XXXXX)'
                function parsePredictionDate(dateString) {{
                    const match = dateString.match(/Date\\((\\d+)\\)/);
                    if (match && match[1]) {{
                        // Chart.js Moment adapter preferă string-uri ISO sau obiecte Date
                        // Aici trebuie să convertim de la formatul Cassandra UDT 'Date(XXXXX)'
                        // Cel mai bine ar fi ca `prediction_date` să fie un string de dată ISO 8601
                        // sau un timestamp direct din Python pentru a evita conversii complexe aici.
                        // Pentru simplitate, vom presupune că 'Date(XXXXX)' este un număr de zile de la epoca UNIX,
                        // sau o altă reprezentare care poate fi convertită în Date.
                        // O soluție mai robustă ar implica trimiterea datelor de dată ca string-uri ISO 8601 din Python.
                        // Moment.js poate parsa direct string-uri YYYY-MM-DD.
                        return dateString; // Lăsăm string-ul așa cum este și ne bazăm pe Moment.js să-l parseze
                    }}
                    return dateString; // Returnează string-ul neschimbat dacă nu este formatul așteptat
                }}

                const ctx = document.getElementById('priceChart').getContext('2d');
                
                // Folosim datele JSON generate separat
                // Parsăm datele pentru a ne asigura că sunt în formatul corect pentru Chart.js (Moment.js)
                const historicalData = {historical_data_json}.map(item => ({{
                    x: item.x, // Moment.js ar trebui să poată parsa 'YYYY-MM-DD'
                    y: item.y
                }}));
                const predictionData = {prediction_data_json}.map(item => ({{
                    x: parsePredictionDate(item.x), // Ajustat pentru a folosi funcția de parsare
                    y: item.y
                }}));
                
                // Consolă.log pentru a verifica datele JavaScript
                console.log('Historical Data:', historicalData);
                console.log('Prediction Data:', predictionData);

                const chartData = {{
                    datasets: [
                        {{
                            label: 'Istoric preț',
                            data: historicalData,
                            borderColor: 'rgb(75, 192, 192)',
                            backgroundColor: 'rgba(75, 192, 192, 0.1)',
                            tension: 0.1,
                            pointRadius: 2,
                            fill: false // Nu umple sub linie
                        }},
                        {{
                            label: 'Predicții',
                            data: predictionData,
                            borderColor: 'rgb(255, 99, 132)',
                            backgroundColor: 'rgba(255, 99, 132, 0.1)',
                            borderDash: [5, 5],
                            tension: 0.1,
                            pointRadius: 4,
                            fill: false // Nu umple sub linie
                        }}
                    ]
                }};
                
                new Chart(ctx, {{
                    type: 'line',
                    data: chartData,
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: {{
                            mode: 'index',
                            intersect: false,
                        }},
                        scales: {{
                            x: {{
                                type: 'time',
                                time: {{ 
                                    unit: 'day',
                                    tooltipFormat: 'YYYY-MM-DD',
                                    displayFormats: {{
                                        day: 'MMM DD'
                                    }}
                                }},
                                title: {{
                                    display: true,
                                    text: 'Dată'
                                }}
                            }},
                            y: {{
                                title: {{
                                    display: true,
                                    text: 'Preț de închidere'
                                }}
                            }}
                        }},
                        plugins: {{
                            title: {{
                                display: true,
                                text: `Evoluția prețului {asset_id} și predicții`,
                                font: {{
                                    size: 16
                                }}
                            }},
                            tooltip: {{
                                callbacks: {{
                                    label: function(context) {{
                                        // Corecția aici: `${{...}}` este pentru Python, iar `$` în interiorul JS backticks
                                        // trebuie să fie `$` simplu.
                                        // Folosim direct context.raw.x și context.raw.y dacă acestea conțin datele originale.
                                        // Altfel, context.parsed.y este valoarea numerică.
                                        return `${{context.dataset.label}}: ${{context.parsed.y.toFixed(2)}}`;
                                    }}
                                }}
                            }}
                        }}
                    }}
                }});
            </script>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    
    except Exception as e:
        import traceback
        traceback.print_exc() # Printează întregul traceback pentru debug
        return HTMLResponse(content=f"<h1>Error: {str(e)}</h1>", status_code=500)

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

# Endpoint pentru datele de bază (preț curent, modificare)
@app.get("/api/dashboard/{asset_id}/summary")
async def get_dashboard_summary(asset_id: str):
    try:
        ts_repo = TimeSeriesRepository(app.state.session)
        historical = ts_repo.find_latest_per_date(
            asset_id, 
            'ALPHAVANTAGE', 
            date.today() - timedelta(days=30),
            date.today()
        )
        
        if not historical:
            return {"error": "Nu există date istorice"}
        
        # Sortăm datele cronologic
        historical_sorted = sorted(historical, key=lambda x: x['timestamp'])
        closes = [float(h['data_values']['close']) for h in historical_sorted]
        
        if len(closes) < 2:
            return {"error": "Date insuficiente pentru calcul"}
        
        current_price = closes[-1]
        prev_close = closes[-2]
        price_change = current_price - prev_close
        percent_change = (price_change / prev_close) * 100
        
        return {
            "current_price": current_price,
            "price_change": price_change,
            "percent_change": percent_change,
            "last_updated": historical_sorted[-1]['timestamp'].isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pentru datele financiare
# Endpoint pentru datele financiare (revizuit)
@app.get("/api/dashboard/{asset_id}/financial")
async def get_financial_data(asset_id: str):
    try:
        # Folosim agregările existente pentru volum
        query = """
        SELECT business_date_year AS year, 
               business_date_month AS month, 
               AVG(avg_volume) AS avg_volume 
        FROM monthly_avg_volume 
        WHERE asset_id = %s
        GROUP BY business_date_year, business_date_month
        ORDER BY year DESC, month DESC
        LIMIT 12
        """
        rows = list(app.state.session.execute(query, [asset_id]))
        
        if not rows:
            return {"error": "Nu există date financiare disponibile"}
        
        # Calculăm media generală
        total_volume = sum(row.avg_volume for row in rows)
        avg_volume = total_volume / len(rows) if len(rows) > 0 else 0
        
        # Returnăm atât datele lunare cât și agregarea
        return {
            "monthly_volumes": [
                {
                    "year": row.year,
                    "month": row.month,
                    "avg_volume": row.avg_volume
                } for row in rows
            ],
            "overall_avg_volume": avg_volume
        }
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

# Endpoint-ul principal pentru dashboard (returnează doar scheletul HTML)
@app.get("/dashboard/{asset_id}", response_class=HTMLResponse)
async def dashboard(asset_id: str):
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ro">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard Financiar - {asset_id}</title>
        <link rel="stylesheet" href="style.css">
        <script src="https://cdn.jsdelivr.net/npm/luxon@3.0.4/build/global/luxon.min.js"></script>
        <script>
            const assetId = "{asset_id}";
            
            // Funcție pentru afișarea stării de încărcare
            function showLoader(containerId) {{
                const container = document.getElementById(containerId);
                if (container) {{
                    container.innerHTML = `
                        <div class="loader">
                            <div class="spinner"></div>
                            <p>Se încarcă datele...</p>
                        </div>
                    `;
                }}
            }}
            
            // Funcție pentru gestionarea erorilor
            function showError(containerId, error) {{
                const container = document.getElementById(containerId);
                if (container) {{
                    container.innerHTML = `
                        <div class="error">
                            <i class="fas fa-exclamation-triangle"></i>
                            <p>Eroare la încărcare: ${{error}}</p>
                        </div>
                    `;
                }}
            }}
            
            // Funcție pentru încărcarea datelor rezumative
            async function loadSummary() {{
                showLoader('header-content');
                try {{
                    const response = await fetch(`/api/dashboard/${{assetId}}/summary`);
                    if (!response.ok) throw new Error('Eroare server');
                    const data = await response.json();
                    
                    // Adăugăm verificări pentru datele lipsă
                    const currentPrice = data.current_price || 0;
                    const priceChange = data.price_change || 0;
                    const percentChange = data.percent_change || 0;

                    document.getElementById('header-content').innerHTML = `
                        <div class="header-top">
                            <h1><i class="fas fa-chart-line"></i> Dashboard Financiar</h1>
                            <div class="ticker">${{assetId}}</div>
                        </div>
                        <div class="header-bottom">
                            <div class="price-display">
                                $${{currentPrice.toFixed(2)}}
                                <span class="${{priceChange >= 0 ? 'change-positive' : 'change-negative'}}">
                                    ${{priceChange >= 0 ? '+' : ''}}${{priceChange.toFixed(2)}} (${{percentChange.toFixed(2)}}%)
                                </span>
                            </div>
                            <p>Ultima actualizare: ${{new Date().toLocaleString()}}</p>
                        </div>
                    `;
                }} catch (error) {{
                    showError('header-content', error.message);
                }}
            }}
            
            // Funcție pentru încărcarea metricilor financiare
            async function loadFinancialMetrics() {{
                showLoader('financial-metrics');
                try {{
                    const response = await fetch(`/api/dashboard/${{assetId}}/financial`);
                    if (!response.ok) throw new Error('Eroare server');
                    const data = await response.json();
                    
                    // Verificăm dacă avem eroare
                    if (data.error) {{
                        document.getElementById('financial-metrics').innerHTML = `
                            <div class="error">
                                <i class="fas fa-exclamation-triangle"></i>
                                <p>${{data.error}}</p>
                            </div>
                        `;
                        return;
                    }}
                    
                    // Formatăm volumul mediu
                    const formatVolume = (vol) => {{
                        if (vol >= 1e9) return `${{(vol/1e9).toFixed(2)}}B`;
                        if (vol >= 1e6) return `${{(vol/1e6).toFixed(2)}}M`;
                        if (vol >= 1e3) return `${{(vol/1e3).toFixed(2)}}K`;
                        return vol.toFixed(2);
                    }};
                    
                    // Creăm conținutul pentru afișare
                    const content = `
                        <h2 class="card-title"><i class="fas fa-chart-bar"></i> Volum Tranzacționat</h2>
                        <div class="metrics-grid">
                            <div class="metric">
                                <div class="metric-label">Volum Mediu (total)</div>
                                <div class="metric-value">${{formatVolume(data.overall_avg_volume)}}</div>
                            </div>
                            
                            ${{data.monthly_volumes.slice(0, 3).map(month => `
                                <div class="metric">
                                    <div class="metric-label">${{month.year}}-${{String(month.month).padStart(2, '0')}}</div>
                                    <div class="metric-value">${{formatVolume(month.avg_volume)}}</div>
                                </div>
                            `).join('')}}
                        </div>
                        
                        <div class="volume-chart-container">
                            <canvas id="volumeChart"></canvas>
                        </div>
                    `;
                    
                    document.getElementById('financial-metrics').innerHTML = content;
                    
                    // Inițializăm graficul după ce elementul există în DOM
                    setTimeout(() => initVolumeChart(data.monthly_volumes), 100);
                }} catch (error) {{
                    showError('financial-metrics', error.message);
                }}
            }}

            // Funcție pentru inițializarea graficului de volum
            function initVolumeChart(monthlyData) {{
                const ctx = document.getElementById('volumeChart').getContext('2d');
                
                // Sortăm datele cronologic
                const sortedData = [...monthlyData].sort((a, b) => {{
                    const dateA = new Date(a.year, a.month - 1);
                    const dateB = new Date(b.year, b.month - 1);
                    return dateA - dateB;
                }});
                
                const labels = sortedData.map(d => 
                    `${{d.year}}-${{String(d.month).padStart(2, '0')}}`
                );
                
                const volumes = sortedData.map(d => d.avg_volume);
                
                new Chart(ctx, {{
                    type: 'bar',
                    data: {{
                        labels: labels,
                        datasets: [{{
                            label: 'Volum mediu tranzacționat',
                            data: volumes,
                            backgroundColor: 'rgba(54, 162, 235, 0.6)',
                            borderColor: 'rgba(54, 162, 235, 1)',
                            borderWidth: 1
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        plugins: {{
                            legend: {{
                                display: false
                            }},
                            tooltip: {{
                                callbacks: {{
                                    label: (context) => 
                                        `Volum: ${{(context.raw/1e6).toFixed(2)}}M`
                                }}
                            }}
                        }},
                        scales: {{
                            y: {{
                                beginAtZero: true,
                                ticks: {{
                                    callback: (value) => 
                                        value >= 1e6 ? `${{(value/1e6).toFixed(1)}}M` : 
                                        value >= 1e3 ? `${{(value/1e3).toFixed(1)}}K` : value
                                }}
                            }}
                        }}
                    }}
                }});
            }}

            // Funcție pentru încărcarea predicțiilor
            async function loadPredictions() {{
                showLoader('predictions-content');
                try {{
                    const response = await fetch(`/api/dashboard/${{assetId}}/predictions`);
                    if (!response.ok) throw new Error('Eroare server');
                    const data = await response.json();
                    
                    let rows = '';
                    if (data.length > 0) {{
                        rows = data.map(p => `
                            <tr>
                                <td>${{p.prediction_date}}</td>
                                <td>$${{p.predicted_close.toFixed(2)}}</td>
                                <td>${{p.model_name}}</td>
                                <td>${{p.formatted_time}}</td>
                            </tr>
                        `).join('');
                    }} else {{
                        rows = '<tr><td colspan="4">Nu există predicții disponibile</td></tr>';
                    }}
                    
                    const content = `
                        <h2 class="card-title"><i class="fas fa-crystal-ball"></i> Predicții</h2>
                        <table>
                            <thead>
                                <tr>
                                    <th>Dată</th>
                                    <th>Preț Prezis</th>
                                    <th>Model</th>
                                    <th>Timp predicție</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${{rows}}
                            </tbody>
                        </table>
                    `;
                    
                    document.getElementById('predictions-content').innerHTML = content;
                }} catch (error) {{
                    showError('predictions-content', error.message);
                }}
            }}
            
            
            // Inițierea încărcării componentelor
            document.addEventListener('DOMContentLoaded', () => {{
                loadSummary();
                loadPredictions();
            }});
        </script>
    </head>
    <body
        <div class="container">
            <header id="header-content">
                <div class="loader">
                    <div class="spinner"></div>
                    <p>Se încarcă datele rezumative...</p>
                </div>
            </header>
            
            <div class="dashboard-grid">
                <div class="card" id="predictions-content">
                    <div class="loader">
                        <div class="spinner"></div>
                        <p>Se încarcă predicțiile...</p>
                    </div>
                </div>
            </div>
            
            <footer>
                <p>Dashboard financiar IBM &copy; {datetime.now().year} | Date actualizate în timp real</p>
            </footer>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)