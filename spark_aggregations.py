import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import (
    expr, col, year, month, avg, max as spark_max, min as spark_min, count
)

if sys.platform.startswith("win"):
    hadoop_home = r"E:\Files\DWarehouse_Python\hadoop"  # <- cale ABSOLUTĂ
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += ";" + os.path.join(hadoop_home, "bin")
    os.environ["HADOOP_COMMON_LIB_NATIVE_DIR"] = os.path.join(hadoop_home, "bin")
    os.environ["HADOOP_OPTS"] = f'-Djava.library.path="{os.path.join(hadoop_home, "bin")}"'

def create_spark_session():
    secure_bundle_path = os.path.abspath("datasecure-connect-dw-cassandra.zip")
    
    if not os.path.exists(secure_bundle_path):
        raise FileNotFoundError(f"Secure bundle not found at {secure_bundle_path}")

    spark = SparkSession.builder \
        .appName("Cassandra Aggregations - DW Project") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.cassandracatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .getOrCreate()

    # Add the secure bundle file to Spark context
    spark.sparkContext.addFile(secure_bundle_path)
    
    # Get the distributed file name (without path)
    distributed_bundle_name = os.path.basename(secure_bundle_path)
    
    # Set Cassandra config using the distributed file name
    spark.conf.set("spark.cassandra.connection.config.cloud.path", distributed_bundle_name)

    print("✅ Spark Version:", spark.version)
    print("✅ Using secure bundle at:", secure_bundle_path)
    print("✅ Distributed as:", distributed_bundle_name)
    return spark

def read_data(spark):
    """Citește datele din Cassandra"""
    keyspace = os.getenv("ASTRA_DB_KEYSPACE")
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="time_series_data", keyspace=keyspace) \
        .load()

def write_to_cassandra(df, table):
    """Scrie DataFrame-ul în Cassandra"""
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=os.getenv("ASTRA_DB_KEYSPACE")) \
        .mode("append") \
        .save()

def run_all_aggregations():
    """Execută toate agregările și salvează rezultatele"""
    spark = create_spark_session()
    df = read_data(spark)
    
    # 1. Număr înregistrări per asset
    asset_counts = df.groupBy("asset_id").agg(count("*").alias("count"))
    write_to_cassandra(asset_counts, "asset_counts")
    
    # 2. Volum mediu tranzacționat per asset
    avg_volume = df.groupBy("asset_id").agg(avg(expr("data_values['volume']")).alias("avg_volume"))
    write_to_cassandra(avg_volume, "avg_volume_per_asset")
    
    # 3. Valoarea maximă (high) și minimă (low) pe an per asset
    df_with_year = df.withColumn("year", year("business_date"))
    high_low_per_year = df_with_year.groupBy("asset_id", "year").agg(
        spark_max(expr("data_values['high']")).alias("max_high"),
        spark_min(expr("data_values['low']")).alias("min_low")
    )
    write_to_cassandra(high_low_per_year, "high_low_per_year")
    
    # 4. Prețul mediu de închidere (close) pe lună per asset
    df_with_month = df_with_year.withColumn("month", month("business_date"))
    monthly_avg_close = df_with_month.groupBy("asset_id", "year", "month").agg(
        avg(expr("data_values['close']")).alias("avg_close")
    )
    write_to_cassandra(monthly_avg_close, "monthly_avg_close")
    
    spark.stop()

if __name__ == "__main__":
    run_all_aggregations()