"""
from_bronze_to_silver_example_job.py
~~~~~~~~~~

Sample usage:
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/sample-project/sample-project-pipeline/process_aws_in_aws_out_example_etl_config.json \
    jobs/sample-project/sample-project-pipeline/python/from_bronze_to_silver_example_job.py <input_parquet_uri_1,input_parquet_uri_2,input_parquet_uri_3,input_parquet_uri_xx> <aws_output_url>
"""

import os
import sys
from dependencies.spark import start_spark
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.functions import col, udf, current_timestamp, current_date
from pyspark.sql.window import Window


def main(input_parquet_files_uris: str, output_uri: str) -> None:
    """Spark preprocess data ETL script.

    This pyspark job loads input data and preprocess it.

    Parameters:
    input_parquet_files_uris: str
        Comma separated paths to input .parquet files.
    output_uri: str
        The output path for the extracted data. Result will be saved as .parquet file.

    Returns: None
    """
    spark, LOG, config = start_spark(
        app_name='Preprocess Data ETL Job',
        spark_config={
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.access.key': os.getenv('AWS_ACCESS_KEY_ID'),
            'spark.hadoop.fs.s3a.secret.key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'spark.hadoop.fs.s3a.endpoint': f"{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com",
            'spark.hadoop.fs.s3.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        }
    )

    LOG.warn('Preprocess Data ETL job is up and running')

    # ETL
    data_sdf = get_data(spark, input_parquet_files_uris)
    final_sdf = process_data(spark, sdf=data_sdf)
    write_to_s3(final_sdf, output_uri)

    LOG.warn('Preprocess Data ETL job SUCCESS')
    spark.stop()
    return None


def get_data(spark: SparkSession, input_parquet_files: str) -> DataFrame:
    paths_list = [p.strip() for p in input_parquet_files.split(',')]
    return spark.read.parquet(*paths_list)


def process_data(spark: SparkSession, sdf: DataFrame) -> DataFrame:

    # ======================
    # 1. Wstępne czyszczenie
    # ======================
    cleaned_sdf = (
        sdf
        .withColumn("Date", F.to_date("Date"))
        .withColumn("last_refresh_date", F.to_date("last_refresh_date"))
        .withColumn("Sale Price", F.col("Sale Price").cast("double"))
        .withColumn("Commission Rate", F.col("Commission Rate").cast("double"))
        .withColumn("Commission Earned", F.col("Commission Earned").cast("double"))
        .withColumn("Car Year", F.col("Car Year").cast("int"))
        .withColumn("processing_timestamp", F.to_timestamp("processing_timestamp"))
        .withColumn("Salesperson", F.trim(F.col("Salesperson")))
        .withColumn("Customer Name", F.initcap(F.col("Customer Name")))
        .dropDuplicates()
    )

    # ========================================
    # 2. Dodanie warstwy biznesowych kolumn
    # ========================================
    enriched_sdf = (
        cleaned_sdf
        .withColumn("sale_year", F.year("Date"))
        .withColumn("sale_month", F.month("Date"))
        .withColumn("sale_quarter", F.quarter("Date"))
        .withColumn("event_timestamp", F.to_timestamp("Date"))
        .withColumn(
            "vehicle_age",
            F.year(F.current_date()) - F.col("Car Year")
        )
        .withColumn(
            "price_category",
            F.when(F.col("Sale Price") < 15000, "low")
             .when(F.col("Sale Price") < 30000, "medium")
             .otherwise("high")
        )
        .withColumn(
            "commission_percentage",
            F.round(F.col("Commission Earned") / F.col("Sale Price") * 100, 2)
        )
    )

    # ==========================================
    # 3. Mockowy słownik — join z tabelą referencyjną
    # ==========================================

    car_make_data = [
        ("Nissan", "Japan", "Mid-range"),
        ("Ford", "USA", "Budget"),
        ("Honda", "Japan", "Mid-range"),
        ("Toyota", "Japan", "Mid-range")
    ]

    car_make_schema = T.StructType([
        T.StructField("Car Make", T.StringType(), True),
        T.StructField("Origin Country", T.StringType(), True),
        T.StructField("Market Segment", T.StringType(), True),
    ])

    car_ref_sdf = spark.createDataFrame(car_make_data, schema=car_make_schema)

    joined_sdf = (
        enriched_sdf
        .join(car_ref_sdf, on="Car Make", how="left")
    )

    # =======================================
    # 4. Mockowy join do Salesperson's region
    # =======================================

    region_data = [
        ("Monica Moore MD", "West"),
        ("Roberto Rose", "East"),
        ("Ashley Ramos", "North"),
        ("Patrick Harris", "South"),
        ("Eric Lopez", "West"),
        ("Terry Perkins MD", "North"),
        ("Ashley Brown", "Central"),
        ("Norma Watkins", "East"),
        ("Scott Parker", "West")
    ]

    region_schema = T.StructType([
        T.StructField("Salesperson", T.StringType()),
        T.StructField("Region", T.StringType())
    ])

    region_sdf = spark.createDataFrame(region_data, schema=region_schema)

    joined_sdf = joined_sdf.join(region_sdf, on="Salesperson", how="left")

    # ===================================================
    # 5. Dodanie KPI — sprzedaż w ujęciu okna analitycznego
    # ===================================================

    w_salesperson = Window.partitionBy("Salesperson").orderBy("Date")
    w_salesperson_all = Window.partitionBy("Salesperson")

    kpi_sdf = (
        joined_sdf
        .withColumn("running_sales", F.sum("Sale Price").over(w_salesperson))
        .withColumn("total_sales_per_salesperson", F.sum("Sale Price").over(w_salesperson_all))
        .withColumn("rank_in_salesperson", F.rank().over(w_salesperson))
        .withColumn("avg_commission_salesperson", F.avg("Commission Earned").over(w_salesperson_all))
    )

    # =======================================
    # 6. Dodanie kolumn silver metadata
    # =======================================

    final_sdf = (
        kpi_sdf
        .withColumn("silver_load_timestamp", F.current_timestamp())
        .withColumn("silver_version", F.lit(1))
        .select(
            F.col("Salesperson").alias("sales_person"),
            F.col("Car Make").alias("car_make"),
            F.col("Date").alias("sale_date"),
            F.col("sale_timestamp"),
            F.col("Customer Name").alias("customer_name"),
            F.col("Car Model").alias("car_model"),
            F.col("Car Year").alias("car_year"),
            F.col("Sale Price").alias("sale_price"),
            F.col("Commission Rate").alias("commission_rate"),
            F.col("Commission Earned").alias("commission_earned"),
            F.col("processing_timestamp"),
            F.col("last_refresh_date"),
            F.col("sale_year"),
            F.col("sale_month"),
            F.col("sale_quarter"),
            F.col("vehicle_age"),
            F.col("price_category"),
            F.col("commission_percentage"),
            F.col("Origin country").alias("origin_country"),
            F.col("Market Segment").alias("market_segment"),
            F.col("Region").alias("region"),
            F.col("running_sales").alias("running_sales"),
            F.col("total_sales_per_salesperson"),
            F.col("rank_in_salesperson"),
            F.col("avg_commission_salesperson"),
            F.col("silver_load_timestamp"),
            F.col("silver_version"),
        )
    )

    final_sdf.show(100, truncate=False)

    return final_sdf


def write_to_s3(df: DataFrame, s3_result_uri: str) -> None:
    (df
     .repartition(1)
     .write
     .mode("overwrite") # other options: "append", "ignore", "error"
     .option("header", True)
     .parquet(s3_result_uri))


if __name__ == "__main__":
    main(
        input_parquet_files_uris=sys.argv[1],
        output_uri=sys.argv[2]
    )