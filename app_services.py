import os
import requests
import time
from datetime import date
from cassandra.cluster import Session
from datetime import datetime
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt
from repositories import AssetsRepository, DataSourceRepository, TimeSeriesRepository

load_dotenv()

class AssetService:
    def __init__(self, session: Session):
        self.repository = AssetsRepository(session)
    
    def create_asset(self, symbol: str) -> dict:
        try:
            # Încercăm crearea cu LWT
            asset = {
                'id': symbol,
                'system_time': datetime.now(),
                'name': symbol,
                'description': f"Asset for {symbol}",
                'attributes': {}
            }
            return self.repository.save(asset)
        except Exception as e:
            # Dacă există deja, returnăm versiunea existentă
            existing_asset = self.repository.find_latest(symbol)
            if existing_asset:
                return existing_asset
            raise
    
    def get_asset(self, asset_id: str) -> dict:
        """Obține ultima versiune a unui asset"""
        return self.repository.find_latest(asset_id)
    
    def get_all_assets(self, asset_id: str) -> list:
        """Obține toate versiunile unui asset"""
        return self.repository.find_all(asset_id)

class DataSourceService:
    def __init__(self, session: Session):
        self.repository = DataSourceRepository(session)
    
    def create_data_source(self, source_name: str) -> dict:
        try:
            data_source = {
                'id': source_name,
                'system_time': datetime.now(),
                'name': source_name,
                'description': f"Data source for {source_name}",
                'attributes': {'open', 'high', 'low', 'close', 'volume'}
            }
            self.repository.save(data_source)
            return data_source
        except Exception as e:
            existing_source = self.repository.find_latest(source_name)
            if existing_source:
                return existing_source
            raise
    
    def get_data_source(self, source_id: str) -> dict:
        """Obține ultima versiune a unei surse de date"""
        return self.repository.find_latest(source_id)

class DataIngestionService:
    def __init__(self, session: Session):
        self.ts_repository = TimeSeriesRepository(session)
        self.asset_service = AssetService(session)
        self.data_source_service = DataSourceService(session)
        self.page_size = 200  # Maxim permis de Alpha Vantage
        self.max_retries = 3
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
    def fetch_alpha_vantage_page(self, symbol: str, page: int = None) -> dict:
        """Extrage o pagină de date de la Alpha Vantage"""
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            raise ValueError("Alpha Vantage API key not found in environment variables")
        
        # Construim URL-ul cu parametrul de outputsize
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        
        # Paginare implicită - Alpha Vantage nu suportă paginare directă
        # Folosim outputsize=full pentru toate datele
        if page is None:
            url += "&outputsize=full"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                error_msg = data.get("Error Message") or data.get("Information", "Unknown error")
                raise Exception(f"Alpha Vantage error: {error_msg}")
            
            return data.get("Time Series (Daily)", {})
        except Exception as e:
            raise
    
    def process_time_series_data(self, time_series: dict, symbol: str, start: date, end: date) -> list:
        """Procesează răspunsul Alpha Vantage și îl transformă în formatul nostru"""
        data_points = []
        for date_str, values in time_series.items():
            business_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Săriți datele în afara intervalului
            if business_date < start or business_date > end:
                continue
            
            # Creați punctul de date
            data_point = {
                'asset_id': symbol,
                'data_source_id': 'ALPHAVANTAGE',
                'business_date_year': business_date.year,
                'business_date': business_date,
                'system_time': datetime.now(),
                'data_values': {
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                }
            }
            data_points.append(data_point)
        
        return data_points
    
    def ingest_data(self, symbol: str, start: str, end: str) -> dict:
        # Asigură existența asset-ului și a sursei de date
        self.asset_service.create_asset(symbol)
        self.data_source_service.create_data_source('ALPHAVANTAGE')
        
        # Extrage toate datele (Alpha Vantage nu are paginare adevărată)
        try:
            time_series = self.fetch_alpha_vantage_page(symbol)
            data_points = self.process_time_series_data(time_series, symbol, 
                                                       datetime.strptime(start, '%Y-%m-%d').date(),
                                                       datetime.strptime(end, '%Y-%m-%d').date())
            
            # Salvează în loturi pentru eficiență
            batch_size = 50
            for i in range(0, len(data_points), batch_size):
                batch = data_points[i:i+batch_size]
                self.ts_repository.save_batch(batch)
                # Respectă limitele de rate (5 cereri/minut)
                time.sleep(12)  # 60 secunde / 5 = 12 secunde între loturi
            
            return {"records_ingested": len(data_points)}
        
        except Exception as e:
            raise Exception(f"Data ingestion failed: {str(e)}")
            
    def get_time_series_data(
        self, 
        asset_id: str, 
        data_source_id: str, 
        year: int = None
    ) -> list:
        """Obține datele de serie temporală pentru un anumit an"""
        if year is None:
            year = datetime.now().year
        
        key = {
            'asset_id': asset_id,
            'data_source_id': data_source_id,
            'business_date_year': year
        }
        return self.ts_repository.find_all(key)