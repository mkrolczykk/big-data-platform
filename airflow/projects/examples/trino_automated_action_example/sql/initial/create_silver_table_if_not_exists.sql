CREATE TABLE IF NOT EXISTS hive.bigdata.car_sales_enriched (
    sales_person VARCHAR,
    car_make VARCHAR,
    sale_date VARCHAR,
    sale_timestamp TIMESTAMP,
    customer_name VARCHAR,
    car_model VARCHAR,
    car_year INTEGER,
    sale_price DOUBLE,
    commission_rate DOUBLE,
    commission_earned DOUBLE,
    processing_timestamp TIMESTAMP,
    last_refresh_date DATE,
    sale_year INTEGER,
    sale_month INTEGER,
    sale_quarter INTEGER,
    vehicle_age INTEGER,
    price_category VARCHAR,
    commission_percentage DOUBLE,
    origin_country VARCHAR,
    market_segment VARCHAR,
    region VARCHAR,
    running_sales DOUBLE,
    total_sales_per_salesperson DOUBLE,
    rank_in_salesperson INTEGER,
    avg_commission_salesperson DOUBLE,
    silver_load_timestamp TIMESTAMP,
    silver_version INTEGER
)
WITH (
    format = 'PARQUET',
    external_location = 's3://bigdata-dev-bucket/02_silver/car_sales_data_enriched/'
)
