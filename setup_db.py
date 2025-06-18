from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import connection, management
from dotenv import load_dotenv
import os

from entites import Asset, DataSource, TimeSeriesData  # asigură-te că numele fișierului este corect: "entities", nu "entites"

def create_tables():
    # Încarcă variabilele din fișierul .env
    load_dotenv()

    ASTRA_DB_ID = os.getenv("ASTRA_DB_ID")
    ASTRA_DB_REGION = os.getenv("ASTRA_DB_REGION")
    ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
    ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")

    # Setează permisiunea pentru schema management (ca să nu mai primești warning)
    os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"

    # Configurare conexiune Astra DB
    cloud_config = {
        'secure_connect_bundle': 'data/secure-connect-dw-cassandra.zip'
    }
    auth_provider = PlainTextAuthProvider('token', ASTRA_DB_APPLICATION_TOKEN)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    # Selectează keyspace-ul tău
    session.set_keyspace(ASTRA_DB_KEYSPACE)

    # Conectează CQLEngine la sesiune
    connection.set_session(session)

    # Creează tabelele
    management.sync_table(Asset)
    management.sync_table(DataSource)
    management.sync_table(TimeSeriesData)

if __name__ == "__main__":
    create_tables()
