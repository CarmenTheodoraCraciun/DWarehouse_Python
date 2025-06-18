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

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Startup code
    session, cluster = get_cassandra_session()
    app.state.session = session
    app.state.cluster = cluster
    app.state.asset_service = AssetService(app.state.session)
    app.state.data_ingestion_service = DataIngestionService(app.state.session)
    
    from initialize_data import initialize_required_data
    initialize_required_data(app.state.session)
    
    yield  # App runs here
    
    # Shutdown code
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
        
        # Setare valori implicite
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
    
@app.get("/dashboard/{asset_id}", response_class=HTMLResponse)
async def dashboard(asset_id: str):
    try:
        # Obține date istorice
        ts_repo = TimeSeriesRepository(app.state.session)
        historical = ts_repo.find_latest_per_date(
            asset_id, 
            'ALPHAVANTAGE', 
            date.today() - timedelta(days=30),
            date.today()
        )
        
        # Obține predicții
        pred_query = "SELECT * FROM predictions WHERE asset_id = %s ORDER BY prediction_date ASC"
        predictions = list(app.state.session.execute(pred_query, [asset_id]))
        
        # Pregătește date pentru grafic
        historical_dates = [h['business_date'].isoformat() for h in historical]
        historical_prices = [h['data_values']['close'] for h in historical]
        
        prediction_dates = [p.prediction_date.isoformat() for p in predictions]
        prediction_prices = [p.predicted_close for p in predictions]
        
        # Crează pagină HTML cu Chart.js
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard {asset_id}</title>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        </head>
        <body>
            <h1>Dashboard pentru {asset_id}</h1>
            
            <div style="width:800px; height:600px">
                <canvas id="priceChart"></canvas>
            </div>
            
            <h2>Predicții recente:</h2>
            <ul>
                {"".join(f"<li>{p.prediction_date}: ${p.predicted_close:.2f}</li>" for p in predictions)}
            </ul>
            
            <script>
                const ctx = document.getElementById('priceChart').getContext('2d');
                
                const chartData = {{
                    datasets: [
                        {{
                            label: 'Istoric preț',
                            data: {json.dumps([
                                {{'x': d, 'y': p}} 
                                for d, p in zip(historical_dates, historical_prices)
                            ])},
                            borderColor: 'rgb(75, 192, 192)',
                            tension: 0.1
                        }},
                        {{
                            label: 'Predicții',
                            data: {json.dumps([
                                {{'x': d, 'y': p}} 
                                for d, p in zip(prediction_dates, prediction_prices)
                            ])},
                            borderColor: 'rgb(255, 99, 132)',
                            borderDash: [5, 5],
                            tension: 0.1
                        }}
                    ]
                }};
                
                new Chart(ctx, {{
                    type: 'line',
                    data: chartData,
                    options: {{
                        scales: {{
                            x: {{
                                type: 'time',
                                time: {{ unit: 'day' }}
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
        return HTMLResponse(content=f"<h1>Error: {str(e)}</h1>", status_code=500)