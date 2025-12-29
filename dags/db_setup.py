from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

with DAG(
    'scylla_db_setup',
    default_args=default_args,
    description='Initialize ScyllaDB Keyspace and Tables',
    schedule_interval='@once', # Run once to set up environment
    catchup=False,
) as dag:

    # 1. Initialize Raw Transactions Table
    init_raw_db = BashOperator(
        task_id='init_raw_transactions_table',
        # Executes the CQL script inside the 'scylla' container
        bash_command='docker exec scylla cqlsh -f /opt/scripts/scylladb/db_init.cql'
    )

    # 2. Initialize Aggregation Table
    init_target_db = BashOperator(
        task_id='init_daily_totals_table',
        # Executes the target initialization script
        bash_command='docker exec scylla cqlsh -f /opt/scripts/scylladb/target_init.cql'
    )

    init_raw_db >> init_target_db