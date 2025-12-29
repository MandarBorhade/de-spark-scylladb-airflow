-- Create the aggregation table
CREATE TABLE IF NOT EXISTS dev.daily_customer_totals (
    customer_id text,
    transaction_date date,
    daily_total double,
    -- Primary Key: Partitioned by customer, sorted by date for efficient time-series lookups
    PRIMARY KEY (customer_id, transaction_date)
) WITH CLUSTERING ORDER BY (transaction_date DESC);