from typing import TypeVar, Generic, List, Optional, Dict, Any, Iterable
from cassandra.cluster import Session
from cassandra.query import BatchStatement, dict_factory
from datetime import datetime, date
import json

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
        IF NOT EXISTS
        """
        result = self.session.execute(query, (
            asset['id'],
            asset['system_time'],
            asset.get('name', ''),
            asset.get('description', ''),
            asset.get('attributes', {})
        ))
        
        if not result.one().applied:
            raise Exception("Asset already exists")
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

    def save(self, entity) -> bool:
        query = """
        INSERT INTO data_source 
        (id, system_time, attributes, created_at, description, name) 
        VALUES (%s, %s, %s, %s, %s, %s)
        IF NOT EXISTS
        """
        result = self.session.execute(query, (
            entity['id'],
            entity['system_time'],
            json.dumps(list(entity.get('attributes', set()))),
            entity.get('created_at', datetime.now()),
            entity['description'],
            entity['name']
        ))
        
        if not result.one().applied:
            raise Exception("Data source already exists")
        return True
    
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
            {k: str(v) for k, v in data_point["data_values"].items()}
        ))
        return data_point
    
    def save_batch(self, data_points: List[Dict]) -> None:
        """Salvează un lot de înregistrări eficient"""
        if not data_points:
            return
        
        # Pregătim interogarea
        query = """
        INSERT INTO time_series_data 
        (asset_id, data_source_id, business_date_year, 
         business_date, system_time, data_values) 
        VALUES (?, ?, ?, ?, ?, ?)
        """
        prepared = self.session.prepare(query)
        
        # Creăm un batch
        batch = BatchStatement()
        
        for point in data_points:
            safe_values = {k: str(v) for k, v in point['data_values'].items()}
            batch.add(prepared, (
                point['asset_id'],
                point['data_source_id'],
                point['business_date_year'],
                point['business_date'],
                point['system_time'],
                safe_values
            ))
        
        # Executăm batch-ul
        self.session.execute(batch)
    
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

    def find_latest_per_date(
        self,
        asset_id: str,
        data_source_id: str,
        start_date: date,
        end_date: date
    ) -> List[Dict]:
        """
        Returnează cea mai recentă versiune pentru fiecare dată într-un interval
        """
        # Obținem toate anii din interval
        years = list(range(start_date.year, end_date.year + 1))
        all_data = []
        
        # Colectăm datele pentru fiecare an
        for year in years:
            key = {
                'asset_id': asset_id,
                'data_source_id': data_source_id,
                'business_date_year': year
            }
            # Filtrăm doar datele din intervalul specificat
            year_data = self.find_all(
                key,
                start_date if year == start_date.year else date(year, 1, 1),
                end_date if year == end_date.year else date(year, 12, 31)
            )
            all_data.extend(year_data)
        
        # Grupăm după dată și selectăm cea mai recentă înregistrare
        latest_per_date = {}
        for row in all_data:
            row_dict = dict(row)
            business_date = row_dict['business_date']
            
            if business_date not in latest_per_date or \
            row_dict['system_time'] > latest_per_date[business_date]['system_time']:
                latest_per_date[business_date] = row_dict
        
        # Sortare descrescătoare după dată
        sorted_dates = sorted(latest_per_date.keys(), reverse=True)
        return [latest_per_date[d] for d in sorted_dates]

    def find_all(
        self, 
        key: Dict, 
        start_date: date = None, 
        end_date: date = None
    ) -> List[Dict]:
        """
        Găsește toate datele de serie temporală cu filtrare opțională
        """
        query = """
        SELECT * FROM time_series_data 
        WHERE asset_id = %s 
        AND data_source_id = %s 
        AND business_date_year = %s
        """
        params = [
            key['asset_id'],
            key['data_source_id'],
            key['business_date_year']
        ]
        
        # Adăugăm filtrele pentru dată dacă sunt specificate
        if start_date:
            query += " AND business_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND business_date <= %s"
            params.append(end_date)
        
        result = self.session.execute(query, tuple(params))
        return list(result)