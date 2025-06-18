from datetime import datetime
import json
from repositories import DataSourceRepository

def initialize_required_data(session):
    data_source_repo = DataSourceRepository(session)
    
    if not data_source_repo.find_latest('ALPHAVANTAGE'):
        attributes = {'open', 'high', 'low', 'close', 'volume'}  # Folosește set, nu listă
        
        new_source = {
            'id': 'ALPHAVANTAGE',
            'system_time': datetime.now(),
            'name': 'Alpha Vantage',
            'description': 'Financial data from Alpha Vantage',
            'attributes': attributes  # Direct set
        }
        data_source_repo.save(new_source)
        print("Created ALPHAVANTAGE data source")
    else:
        print("ALPHAVANTAGE data source already exists")