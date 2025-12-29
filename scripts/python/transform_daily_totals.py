import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Logging Setup ---
LOG_DIR = "/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(filename=f"{LOG_DIR}/transform_daily_totals.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder \
    .appName("DeltaTransformationFix") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

bronze_path = "/opt/spark/work-dir/delta-lake/customer_transactions"
silver_path = "/opt/spark/work-dir/delta-lake/daily_customer_totals"

logging.info("STARTING TRANSFORMATION: BRONZE TO SILVER")
raw_df = spark.read.format("delta").load(bronze_path)

cleaned_df = raw_df.dropDuplicates(["transaction_id"]) \
    .filter((F.col("amount") > 0) & (F.col("customer_id").isNotNull()))

cleaned_df = cleaned_df.withColumn(
    "transaction_date_parsed", 
    F.coalesce(
        F.to_date(F.col("transaction_date"), "yyyy-MM-dd"),
        F.to_date(F.col("transaction_date"), "dd/MM/yyyy")
    )
)

daily_totals_df = cleaned_df.groupBy("customer_id", "transaction_date_parsed") \
    .agg(F.round(F.sum("amount"), 2).alias("daily_total")) \
    .withColumnRenamed("transaction_date_parsed", "transaction_date")

logging.info(f"WRITING TO SILVER TABLE: DAILY_CUSTOMER_TOTALS")
daily_totals_df.write.format("delta").mode("append").save(silver_path)
logging.info(f"TRANSFORMATION COMPLETE: {daily_totals_df.count()}")

spark.stop()