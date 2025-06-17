from fastapi import FastAPI, HTTPException, Query, Path
from contextlib import asynccontextmanager
from database import get_cassandra_session
from app_services import AssetService, DataIngestionService
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import AsyncIterator

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
            "records_ingested": result.get("records", 0)
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
    year: int = Query(None, title="Anul pentru care se cer datele")
):
    try:
        from repositories import TimeSeriesRepository
        repo = TimeSeriesRepository(app.state.session)
        key = {
            'asset_id': asset_id,
            'data_source_id': data_source_id,
            'business_date_year': year or datetime.now().year
        }
        return repo.find_all(key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))