import os
import logging
from pyspark.sql import SparkSession

# --- Logging Setup ---
LOG_DIR = "/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=f"{LOG_DIR}/ingest_to_scylla.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Spark 
spark = SparkSession.builder \
    .appName("DeltaToScyllaIngestion") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.cassandra.connection.host", "scylla") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Path to Delta Lake table
delta_path = "/opt/spark/work-dir/delta-lake/customer_transactions"

try:
    logging.info("Starting ingestion process: Reading from Delta Lake.")
    # Read Delta table
    df = spark.read.format("delta").load(delta_path)

    logging.info(f"INGESTING DATA INTO SCYLLADB | TABLE: 'customer_transactions'")
    
    # Write DataFrame into Scylla
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_transactions", keyspace="dev") \
        .save()

    logging.info(f"DATA INGESTION COMPLETE | COUNT {df.count()}")

except Exception as e:
    logging.error(f"DATA INGESTION FAILED SCYLLADB: {str(e)}")
    print(f"Error occurred: {e}")

finally:
    logging.info(f"SPARK SESSION: {spark}")
    spark.stop()