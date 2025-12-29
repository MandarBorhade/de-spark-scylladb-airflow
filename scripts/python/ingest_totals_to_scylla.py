import os
import logging
import sys
from pyspark.sql import SparkSession

# --- Logging Setup ---
LOG_DIR = "/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/ingest_totals_to_scylla.log"),
        # logging.StreamHandler(sys.stdout) 
    ],
    force=True 
)

logger = logging.getLogger(__name__)

# Initialize Spark
spark = SparkSession.builder \
    .appName("ScyllaIngestionFinal") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "scylla") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Read the aggregated Delta table
silver_path = "/opt/spark/work-dir/delta-lake/daily_customer_totals"

try:
    logger.info("READING DAILY TOTALS FROM DELTA LAKE PATH: %s", silver_path)
    df = spark.read.format("delta").load(silver_path)

    logger.info(f"INGESTING DAILY TOTALS INTO SCYLLADB TABLE: daily_customer_totals | COUNT: {df.count()}")
    # Write to Scylla
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="daily_customer_totals", keyspace="dev") \
        .save()
    
    logger.info("SCYLLADB INGESTION SUCCESSFUL.")

except Exception as e:
    logger.error("SCYLLADB INGESTION FAILED: %s", str(e), exc_info=True)
    raise e 
finally:
    logger.info(f"STOPPING SPARK SESSION | {spark}")
    spark.stop()