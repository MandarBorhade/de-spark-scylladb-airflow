import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- Logging Setup ---
LOG_DIR = "/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(filename=f"{LOG_DIR}/write_delta.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder \
    .appName("WriteCustomerTransactionsData") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("merchant", StringType(), True)
])

landing_zone_path = "/opt/spark/work-dir/delta-lake/landing-zone"
delta_path = "/opt/spark/work-dir/delta-lake/customer_transactions"

try:
    logging.info(f"READING CSV FROM LANDING ZONE | {landing_zone_path}")
    df = spark.read.option("header", "true").schema(schema).csv(landing_zone_path)
    
    logging.info(f"WRITING TO DELTA LAKE: {df.count()}")
    df.write.format("delta").mode("overwrite").save(delta_path)
    logging.info(f"ADDED DATA TO: {delta_path}")
except Exception as e:
    logging.error(f"ERROR WRITING TO DELTA: {str(e)}")
finally:
    spark.stop()