-- Create the keyspace
CREATE KEYSPACE IF NOT EXISTS dev 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create the table
CREATE TABLE dev.customer_transactions (
    transaction_id text PRIMARY KEY,
    customer_id text,
    amount double,              
    transaction_date text,
    merchant text               
);