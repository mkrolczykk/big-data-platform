CREATE TABLE IF NOT EXISTS hive.bigdata.car_sales_data_aggr (
    sales_month VARCHAR,
    sales_year INTEGER,
    car_make VARCHAR,
    market_segment VARCHAR,
    origin_country VARCHAR,
    total_sales DOUBLE,
    avg_sale_price DOUBLE,
    avg_commission DOUBLE,
    total_transactions INTEGER,
    brand_sales_share DOUBLE,
    salesperson_rank INTEGER,
    gold_load_timestamp TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bigdata-dev-bucket/03_gold/car_sales_data_aggr/'
)