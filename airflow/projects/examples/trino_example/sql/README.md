# Trino cluster (Superset connection)
```
trino://trino@host.docker.internal:8080/hive
```

## SQL samples:

```
show catalogs;
```

```
show schemas from hive;
```

```
show tables from hive.bigdata;
```

```
-- Tworzenie schematu
CREATE SCHEMA IF NOT EXISTS hive.bigdata
WITH (location = 's3a://bigdata-dev-bucket/');
```

```
-- Tworzenie tabeli źródłowej
CREATE TABLE IF NOT EXISTS hive.bigdata.car_sales_bronze (
    "Date" VARCHAR,
    "Salesperson" VARCHAR,
    "Customer Name" VARCHAR,
    "Car Make" VARCHAR,
    "Car Model" VARCHAR,
    "Car Year" STRING,
    "Sale Price" STRING,
    "Commission Rate" STRING,
    "Commission Earned" STRING,
    processing_timestamp TIMESTAMP,
    last_refresh_date  DATE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bigdata-dev-bucket/01_bronze/car_sales_data_processed/'
);
```

```
SELECT * FROM hive.bigdata.car_sales_bronze;
```

```
-- Wstawianie przekształconych danych
--INSERT INTO hive.bigdata.car_sales_silver
-- Wybrane zapytanie SQL
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
```

```
-- Tworzenie tabeli w warstwie silver
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
```