-- Tworzenie schematu
CREATE SCHEMA IF NOT EXISTS hive.bigdata
WITH (location = 's3a://bigdata-dev-bucket/');

-- Tworzenie tabeli źródłowej
CREATE TABLE IF NOT EXISTS hive.bigdata.car_sales_bronze (
    sale_date DATE,
    salesperson_name VARCHAR,
    customer_name VARCHAR,
    make VARCHAR,
    model VARCHAR,
    year INTEGER,
    price INTEGER,
    discount DOUBLE,
    total_amount DOUBLE,
    created_at TIMESTAMP,
    updated_at DATE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bigdata-dev-bucket/01_bronze/car_sales_data_processed/'
);

-- Sprawdzenie czy są dane w tabeli źródłowej
SELECT COUNT(*) as source_count FROM hive.bigdata.car_sales_bronze;

-- Tworzenie tabeli docelowej
CREATE TABLE IF NOT EXISTS hive.bigdata.car_sales_silver (
    sale_date DATE,
    salesperson_name VARCHAR,
    customer_name VARCHAR,
    make VARCHAR,
    model VARCHAR,
    year INTEGER,
    price INTEGER,
    discount DOUBLE,
    total_amount DOUBLE,
    created_at TIMESTAMP,
    updated_at DATE,
    sale_year INTEGER,
    sale_month INTEGER,
    sale_quarter INTEGER,
    discounted_price DOUBLE,
    price_category VARCHAR,
    days_since_sale BIGINT
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bigdata-dev-bucket/02_silver/car_sales_data_enriched/'
);

-- Wstawianie przekształconych danych
INSERT INTO hive.bigdata.car_sales_silver
SELECT
    sale_date,
    salesperson_name,
    customer_name,
    make,
    model,
    year,
    price,
    discount,
    total_amount,
    created_at,
    updated_at,
    YEAR(sale_date) AS sale_year,
    MONTH(sale_date) AS sale_month,
    QUARTER(sale_date) AS sale_quarter,
    ROUND(CAST(price AS DOUBLE) * (1 - discount), 2) AS discounted_price,
    CASE
        WHEN price < 20000 THEN 'Budget'
        WHEN price BETWEEN 20000 AND 50000 THEN 'Mid-range'
        ELSE 'Premium'
    END AS price_category,
    DATE_DIFF('day', sale_date, CURRENT_DATE) AS days_since_sale
FROM hive.bigdata.car_sales_bronze;

-- Sprawdzenie wyników
SELECT 'Transformation completed successfully!' as status;
SELECT COUNT(*) as total_records FROM hive.bigdata.car_sales_silver;
SELECT make, model, COUNT(*) as count
FROM hive.bigdata.car_sales_silver
GROUP BY make, model
LIMIT 10;