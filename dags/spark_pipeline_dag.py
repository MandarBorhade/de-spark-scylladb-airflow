from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1), # Adjusted for 2025
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transaction_etl_pipeline',
    default_args=default_args,
    description='End-to-end Data Generation and Spark Processing',
    schedule_interval=None, # Set to None for manual triggering
    catchup=False,
) as dag:

    # 1. Generate Raw CSV Data
    # Creates 5 files with 400 records each into the landing zone
    generate_data = BashOperator(
        task_id='generate_raw_csv',
        bash_command='docker exec spark-master python3 /opt/spark/work-dir/scripts/python/generate_csv.py'
    )

    # 2. Load CSV to Delta (Bronze)
    write_bronze = BashOperator(
        task_id='ingest_csv_to_delta',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/spark/work-dir/scripts/python/write_delta.py'
    )

    # 3. Bronze Delta to Scylla (Raw Transactions)
    # Note: Requires transaction_id primary key
    ingest_raw_scylla = BashOperator(
        task_id='ingest_raw_to_scylla',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/spark/work-dir/scripts/python/ingest_to_scylla.py'
    )

    # 4. Bronze to Silver Transformation (Aggregates)
    # Handles data quality: null customer_id, negative amounts, and date formatting
    transform_silver = BashOperator(
        task_id='transform_to_daily_totals',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/spark/work-dir/scripts/python/transform_daily_totals.py'
    )

    # 5. Silver Delta to Scylla (Final Aggregates)
    # Note: Requires customer_id and transaction_date primary key
    ingest_totals_scylla = BashOperator(
        task_id='ingest_totals_to_scylla',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.cassandra.connection.host=scylla" /opt/spark/work-dir/scripts/python/ingest_totals_to_scylla.py'
    )

    # Workflow Dependency
    generate_data >> write_bronze >> ingest_raw_scylla >>  transform_silver >> ingest_totals_scylla