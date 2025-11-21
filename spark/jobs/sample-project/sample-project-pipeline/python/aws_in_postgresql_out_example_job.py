"""
aws_in_postgresql_out_example_job.py
~~~~~~~~~~

Sample usage:
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/sample-project/sample-project-pipeline/aws_in_postgresql_out_example_etl_config.json \
    jobs/sample-project/sample-project-pipeline/python/aws_in_postgresql_out_example_job.py <input_csv_uri_1,input_csv_uri_2,input_csv_uri_3,input_csv_uri_xx> <postgresql_output_url>
"""

import os
import sys
from dependencies.spark import start_spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, current_timestamp, current_date
from pyspark.sql.types import StringType


def main(input_csv_files_uris: str, postgresql_output_url: str) -> None:
    """Spark preprocess data ETL script.

    This pyspark job loads input data and preprocess it.

    Parameters:
    input_csv_files: str
        Comma separated paths to input CSV files.
    postgresql_output_url: str
        The output url for postgreSQL database.

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
        },
        jar_packages=['org.postgresql:postgresql:42.6.0'],
    )

    LOG.warn('Preprocess Data ETL job is up and running')

    # ETL
    data_sdf = get_data(spark, input_csv_files_uris)
    final_sdf = process_data(sdf=data_sdf)
    write_to_postgresql(final_sdf, postgresql_output_url)

    LOG.warn('Preprocess Data ETL job SUCCESS')
    spark.stop()
    return None


def get_data(spark: SparkSession, input_csv_files: str) -> DataFrame:
    paths_list = input_csv_files.split(',')
    return spark.read.option('header', True).csv(paths_list)


def process_data(sdf: DataFrame) -> DataFrame:

    final_sdf = (
        sdf
        # [...] data cleaning, deduplication, and other...
        .withColumn('processing_timestamp', current_timestamp())
        .withColumn('last_refresh_date', current_date())
    )

    final_sdf.show(100, truncate=False)

    return final_sdf


def write_to_postgresql(sdf: DataFrame, postgresql_output_url: str) -> None:

    # db schema name (hardcoded, only for presentation)
    schema_name = "public"

    # Tables (hardcoded, only for presentation):
    # 1. electric_production
    # 2. monthly_beer_production_in_austr
    # 3. car_sales
    table_name = "car_sales"

    (
        sdf
        .write
        .format("jdbc")
        .mode("overwrite")  # other options: "append", "ignore", "error"
        .option("url", postgresql_output_url)
        .option("dbtable", f"{schema_name}.{table_name}")
        .option("user", os.getenv('POSTGRES_USER'))
        .option("password", os.getenv('POSTGRES_PASSWORD'))
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 250)  # batch size
        .option("isolationLevel", "READ_COMMITTED")
        .save()
    )


if __name__ == "__main__":
    main(
        input_csv_files_uris=sys.argv[1],
        postgresql_output_url=sys.argv[2]
    )