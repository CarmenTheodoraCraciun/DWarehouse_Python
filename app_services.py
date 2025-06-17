import os
import requests
import time
from cassandra.cluster import Session
from datetime import datetime
from dotenv import load_dotenv
# from cassandra_service.cassandra_service import CassandraService
from repositories import AssetsRepository, DataSourceRepository, TimeSeriesRepository

load_dotenv()

class AssetService:
    def __init__(self, session: Session):
        self.repository = AssetsRepository(session)
    
    def create_asset(self, symbol: str) -> dict:
        existing_asset = self.repository.find_latest(symbol)
        if existing_asset:
            return existing_asset
        
        asset = {
            'id': symbol,
            'system_time': datetime.now(),
            'name': symbol,
            'description': f"Asset for {symbol}",
            'attributes': {}
        }
        return self.repository.save(asset)
    
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
        existing_source = self.repository.find_latest(source_name)
        if existing_source:
            return existing_source
        
        data_source = {
            'id': source_name,
            'system_time': datetime.now(),
            'name': source_name,
            'description': f"Data source for {source_name}",
            'attributes': {'open', 'high', 'low', 'close', 'volume'}
        }
        return self.repository.save(data_source)
    
    def get_data_source(self, source_id: str) -> dict:
        """Obține ultima versiune a unei surse de date"""
        return self.repository.find_latest(source_id)

class DataIngestionService:
    def __init__(self, session: Session):
        self.ts_repository = TimeSeriesRepository(session)
        self.asset_service = AssetService(session)
        self.data_source_service = DataSourceService(session)
    
    def ingest_data(self, symbol: str, start: str, end: str) -> dict:
        # Ensure asset exists
        self.asset_service.create_asset(symbol)
        
        # Ensure data source exists
        self.data_source_service.create_data_source('ALPHAVANTAGE')
        
        # Get API key from environment
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            raise ValueError("Alpha Vantage API key not found in environment variables")
        
        # Fetch data from Alpha Vantage
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=full"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                raise Exception(f"Alpha Vantage error: {data['Error Message']}")
            
            time_series = data.get("Time Series (Daily)", {})
            records_ingested = 0
            
            for date_str, values in time_series.items():
                # Parse date
                business_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                start_date = datetime.strptime(start, '%Y-%m-%d').date()
                end_date = datetime.strptime(end, '%Y-%m-%d').date()
                
                # Skip dates outside requested range
                if business_date < start_date or business_date > end_date:
                    continue
                
                # Create data point
                data_point = {
                    'asset_id': symbol,
                    'data_source_id': 'ALPHAVANTAGE',
                    'business_date_year': business_date.year,
                    'business_date': business_date,
                    'system_time': datetime.now(),
                    'data_values': {
                        'open': values['1. open'],
                        'high': values['2. high'],
                        'low': values['3. low'],
                        'close': values['4. close'],
                        'volume': values['5. volume']
                    }
                }
                
                # Save to Cassandra
                self.ts_repository.save(data_point)
                records_ingested += 1
                
                # Respect API rate limits (5 requests/minute)
                if records_ingested % 5 == 0:
                    time.sleep(60)
            
            return {"records_ingested": records_ingested}
        
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