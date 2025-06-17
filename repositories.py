from typing import TypeVar, Generic, List, Optional, Dict, Any, Iterable
from cassandra.cluster import Session
from cassandra.query import SimpleStatement, dict_factory
from datetime import datetime

E = TypeVar('E')  # Entity type
K = TypeVar('K')  # Key type

class WarehouseRepository(Generic[E, K]):
    def __init__(self, session: Session, table_name: str):
        self.session = session
        self.table_name = table_name
        self.session.row_factory = dict_factory

    def save(self, entity: E) -> E:
        raise NotImplementedError

    def delete(self, entity: E) -> None:
        raise NotImplementedError

    def delete_all(self, key: K) -> None:
        raise NotImplementedError

    def find_latest(self, key: K) -> Optional[E]:
        raise NotImplementedError

    def find_all(self, key: K) -> Iterable[E]:
        raise NotImplementedError


class AssetsRepository(WarehouseRepository):
    def __init__(self, session: Session):
        super().__init__(session, "asset")

    def save(self, asset: Dict) -> Dict:
        query = """
        INSERT INTO asset 
        (id, system_time, name, description, attributes) 
        VALUES (%s, %s, %s, %s, %s)
        """
        self.session.execute(query, (
            asset['id'],
            asset['system_time'],
            asset.get('name', ''),
            asset.get('description', ''),
            asset.get('attributes', {})
        ))
        return asset

    def delete(self, asset: Dict) -> None:
        query = "DELETE FROM asset WHERE id = %s AND system_time = %s"
        self.session.execute(query, (asset['id'], asset['system_time']))

    def delete_all(self, id: str) -> None:
        query = "DELETE FROM asset WHERE id = %s"
        self.session.execute(query, (id,))

    def find_latest(self, id: str) -> Optional[Dict]:
        query = """
        SELECT * FROM asset 
        WHERE id = %s 
        ORDER BY system_time DESC 
        LIMIT 1
        """
        result = self.session.execute(query, (id,))
        return result.one()

    def find_all(self, id: str) -> List[Dict]:
        query = "SELECT * FROM asset WHERE id = %s"
        result = self.session.execute(query, (id,))
        return list(result)


class DataSourceRepository(WarehouseRepository):
    def __init__(self, session: Session):
        super().__init__(session, "data_source")

    def save(self, entity):
        query = f"""
        INSERT INTO {self.table_name} 
        (id, system_time, attributes, created_at, description, name) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.session.execute(query, (
            entity['id'],
            entity['system_time'],
            entity.get('attributes', set()),  # Folosește set gol ca default
            entity.get('created_at', datetime.now()),
            entity['description'],
            entity['name']
        ))
        return query
        
    def delete(self, data_source: Dict) -> None:
        query = "DELETE FROM data_source WHERE id = %s AND system_time = %s"
        self.session.execute(query, (data_source['id'], data_source['system_time']))

    def delete_all(self, id: str) -> None:
        query = "DELETE FROM data_source WHERE id = %s"
        self.session.execute(query, (id,))

    def find_latest(self, id: str) -> Optional[Dict]:
        query = """
        SELECT * FROM data_source 
        WHERE id = %s 
        ORDER BY system_time DESC 
        LIMIT 1
        """
        result = self.session.execute(query, (id,))
        return result.one()

    def find_all(self, id: str) -> List[Dict]:
        query = "SELECT * FROM data_source WHERE id = %s"
        result = self.session.execute(query, (id,))
        return list(result)


class TimeSeriesRepository(WarehouseRepository):
    def __init__(self, session: Session):
        super().__init__(session, "time_series_data")

    def save(self, data_point: Dict) -> Dict:
        query = """
        INSERT INTO time_series_data 
        (asset_id, data_source_id, business_date_year, 
         business_date, system_time, data_values) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.session.execute(query, (
            data_point['asset_id'],
            data_point['data_source_id'],
            data_point['business_date_year'],
            data_point['business_date'],
            data_point['system_time'],
            data_point['data_values']  # Already a map
        ))
        return data_point

    def delete(self, data_point: Dict) -> None:
        query = """
        DELETE FROM time_series_data 
        WHERE asset_id = %s AND data_source_id = %s 
        AND business_date_year = %s AND business_date = %s
        AND system_time = %s
        """
        self.session.execute(query, (
            data_point['asset_id'],
            data_point['data_source_id'],
            data_point['business_date_year'],
            data_point['business_date'],
            data_point['system_time']
        ))

    def delete_all(self, key: Dict) -> None:
        query = """
        DELETE FROM time_series_data 
        WHERE asset_id = %s AND data_source_id = %s 
        AND business_date_year = %s
        """
        self.session.execute(query, (
            key['asset_id'],
            key['data_source_id'],
            key['business_date_year']
        ))

    def find_latest(self, key: Dict) -> Optional[Dict]:
        query = """
        SELECT * FROM time_series_data 
        WHERE asset_id = %s AND data_source_id = %s 
        AND business_date_year = %s 
        ORDER BY business_date DESC, system_time DESC 
        LIMIT 1
        """
        result = self.session.execute(query, (
            key['asset_id'],
            key['data_source_id'],
            key['business_date_year']
        ))
        return result.one()

    def find_all(self, key: Dict) -> List[Dict]:
        query = """
        SELECT * FROM time_series_data 
        WHERE asset_id = %s AND data_source_id = %s 
        AND business_date_year = %s
        """
        result = self.session.execute(query, (
            key['asset_id'],
            key['data_source_id'],
            key['business_date_year']
        ))
        return list(result)