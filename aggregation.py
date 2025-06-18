import os
from collections import defaultdict
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_BUNDLE = "data/secure-connect-{}.zip".format(os.getenv("ASTRA_DB_ID"))

cloud_config = {'secure_connect_bundle': ASTRA_DB_BUNDLE}
auth_provider = PlainTextAuthProvider("token", ASTRA_DB_TOKEN)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace(ASTRA_DB_KEYSPACE)

print("✅ Connected to Cassandra.")

# ——— Aggregare 1: COUNT(*) per asset & year ———
rows = session.execute("""
    SELECT asset_id, business_date_year
    FROM data
    WHERE data_source_id = 'NASDAQ-DATA-LINK.QDL/BITFINEX'
    ALLOW FILTERING
""")
counts = defaultdict(int)
for row in rows:
    counts[(row.asset_id, row.business_date_year)] += 1

for (asset_id, year), count in counts.items():
    session.execute("""
        INSERT INTO totals (asset_id, business_date_year, cnt)
        VALUES (%s, %s, %s)
    """, (asset_id, year, count))
print("✅ Aggregare 1 scrisă în tabela `totals`.")

# ——— Aggregare 2: AVG(volume) per asset & lună ———
rows2 = session.execute("""
    SELECT asset_id, business_date_year, business_date_month, data_values
    FROM data
    ALLOW FILTERING
""")

agg2 = defaultdict(list)
for row in rows2:
    try:
        volume = row.data_values.get("volume")
        if volume is not None:
            k = (row.asset_id, row.business_date_year, row.business_date_month)
            agg2[k].append(volume)
    except Exception:
        continue

for (asset_id, year, month), volumes in agg2.items():
    avg_volume = sum(volumes) / len(volumes)
    session.execute("""
        INSERT INTO monthly_avg_volume (asset_id, business_date_year, business_date_month, avg_volume)
        VALUES (%s, %s, %s, %s)
    """, (asset_id, year, month, avg_volume))
print("✅ Aggregare 2 scrisă în tabela `monthly_avg_volume`.")

session.shutdown()
print("✅ Gata! Cassandra închisă. Agregările sunt live. 🚀")
