import time
import random
import os
import logging
from datetime import datetime
from faker import Faker
import pandas as pd

# --- Logging Setup ---
LOG_DIR = "/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=f"{LOG_DIR}/generate_csv.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def generate_timestamp_id():
    ts = datetime.now()
    unique_id = f"{ts.strftime('%Y%m%d%H%M%S')}{ts.microsecond:06d}{random.randint(100, 999)}"
    return unique_id

def generate_csv_batch(num_records):
    fake = Faker()
    output_dir = "/opt/spark/work-dir/delta-lake/landing-zone"
    os.makedirs(output_dir, exist_ok=True)
    
    data = []
    cust_ids = [f"C{random.randint(10000, 99999)}" for _ in range(50)]
    merchants = ["STORE_23", "ONLINE_SHOP", "GAS_STATION", "TECH_MART", "COFFEE_HOUSE"]

    for i in range(num_records):
        tx_id = generate_timestamp_id()
        c_id = random.choice(cust_ids)
        amt = round(random.uniform(5.0, 500.0), 2)
        
        if i % 50 == 0: tx_id = data[-1][0] if len(data) > 0 else tx_id
        if i % 75 == 0: amt = -amt
        if i % 30 == 0: c_id = None
        if i % 40 == 0: merchants.append("???INVALID###")
        if i % 60 == 0:
            ts = fake.date_time_between(start_date='-1d', end_date='now').strftime('%d/%m/%Y %H:%M')
        else:
            ts = fake.date_time_between(start_date='-1d', end_date='now').strftime('%Y-%m-%dT%H:%M:%SZ')
        if i % 90 == 0: amt = amt * 1000

        data.append([tx_id, c_id, amt, ts, random.choice(merchants)])
        time.sleep(0.0001)

    df = pd.DataFrame(data, columns=["transaction_id", "customer_id", "amount", "timestamp", "merchant"])
    file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    file_path = f"{output_dir}/transactions_batch_{file_timestamp}.csv"
    df.to_csv(file_path, index=False)
    logging.info(f"GENERATED: {num_records} RECORDS | FILE PATH: {file_path}")

if __name__ == "__main__":
    num_files = 5
    records_per_file = 400
    logging.info(f"STARTING DATA GENERATION: {num_files} files.")
    for i in range(num_files):
        generate_csv_batch(records_per_file)
    logging.info("DATA GENERATION: COMPLETE")