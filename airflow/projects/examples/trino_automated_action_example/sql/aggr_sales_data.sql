-- INSERT INTO hive.bigdata.car_sales_data_aggr
WITH aggr_data AS (
    SELECT
        CONCAT(CAST(sale_year AS VARCHAR), '-', LPAD(CAST(sale_month AS VARCHAR), 2, '0')) AS sales_month,
        sale_year,
        car_make,
        market_segment,
        origin_country,
        SUM(sale_price) AS total_sales,
        AVG(sale_price) AS avg_sale_price,
        AVG(commission_earned) AS avg_commission,
        COUNT(*) AS total_transactions,
        SUM(sale_price) / SUM(SUM(sale_price)) OVER (PARTITION BY sale_year, sale_month) AS brand_sales_share,
        ROW_NUMBER() OVER (PARTITION BY sale_year, sale_month ORDER BY SUM(sale_price) DESC) AS salesperson_rank,
        CURRENT_TIMESTAMP AS gold_load_timestamp,
        CURRENT_DATE AS load_date
    FROM hive.bigdata.car_sales_enriched
    GROUP BY sale_year, sale_month, car_make, market_segment, origin_country
)
SELECT
    sales_month,
    sale_year AS sales_year,
    car_make,
    market_segment,
    origin_country,
    total_sales,
    avg_sale_price,
    avg_commission,
    total_transactions,
    brand_sales_share,
    salesperson_rank,
    gold_load_timestamp
FROM aggr_data
-- WHERE CAST(gold_load_timestamp AS DATE) = CURRENT_DATE
WHERE
    total_sales >= {{ params.total_sales_floor_value }}
ORDER BY total_sales DESC
LIMIT {{ params.bonus_limit }}