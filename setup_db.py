from cassandra.cluster import Cluster
from cassandra.cqlengine import management
from entites import Asset, DataSource, TimeSeriesData

def create_tables():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    
    # Create keyspace
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS dw_project 
    WITH replication = {
        'class': 'SimpleStrategy', 
        'replication_factor': 1
    }
    """)
    
    session.execute("USE dw_project")
    
    # Sync tables
    management.sync_table(Asset)
    management.sync_table(DataSource)
    management.sync_table(TimeSeriesData)

if __name__ == "__main__":
    create_tables()