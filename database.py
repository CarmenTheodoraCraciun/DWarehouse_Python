import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")

def get_cassandra_session():
    cloud_path = './data/secure-connect-dw-cassandra.zip'

    cloud_config = {
        'secure_connect_bundle': cloud_path
    }

    auth_provider = PlainTextAuthProvider('token', ASTRA_DB_APPLICATION_TOKEN)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    # selectăm keyspace-ul
    session.set_keyspace(ASTRA_DB_KEYSPACE)

    return session, cluster
