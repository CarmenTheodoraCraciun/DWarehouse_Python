from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv

load_dotenv()

class CassandraService:
    def __init__(self):
        self.cluster = None
        self.session = None
        
    def connect(self):
        cloud_config = {
            'secure_connect_bundle': 'data/secure-connect-dw-cassandra.zip'
        }
        auth_provider = PlainTextAuthProvider(
            'token',
            os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        )
        self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = self.cluster.connect(os.getenv("ASTRA_DB_KEYSPACE"))
        
    def get_session(self):
        if not self.session:
            self.connect()
        return self.session
    
    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            
    def execute_query(self, query, params=None):
        try:
            return self.get_session().execute(query, params)
        except Exception as e:
            print(f"Eroare la executarea query: {e}")
            raise

# Inițializează serviciul global
cassandra_service = CassandraService()