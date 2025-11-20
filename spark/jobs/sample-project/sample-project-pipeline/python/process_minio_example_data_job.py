"""
aggr_ur_swap_events_data_etl_job.py
~~~~~~~~~~

Sample usage:
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/sample-project/sample-project-pipeline/process_minio_example_data_etl_config.json \
    jobs/sample-project/sample-project-pipeline/python/process_minio_example_data_job.py <input_csv_uri_1,input_csv_uri_2,input_csv_uri_3,input_csv_uri_xx> <output_uri_for_csv_with_success_rows>
"""

import os
import sys
from dependencies.spark import start_spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType


def main(input_csv_files_uris: str, output_uri: str) -> None:
    """Spark preprocess data ETL script.

    This pyspark job loads input data and preprocess it.

    Parameters:
    input_csv_files: str
        Comma separated paths to input CSV files.
    output_uri: str
        The output path for the extracted data. Result will be saved as .csv file.

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
    data_df = get_data(spark, input_csv_files_uris)
    success_rows_sdf = process_data(df=data_df)
    write_to_s3(success_rows_sdf, output_uri)

    LOG.warn('Preprocess Data ETL job SUCCESS')
    spark.stop()
    return None


def get_data(spark: SparkSession, input_csv_files: str) -> DataFrame:
    paths_list = input_csv_files.split(',')
    return spark.read.option('header', True).csv(paths_list)


def process_data(df: DataFrame) -> (DataFrame, DataFrame):

    final_sdf = df.withColumn('processed_time', current_timestamp())

    final_sdf.show(100, truncate=False)

    return final_sdf


def write_to_s3(df: DataFrame, s3_result_uri: str) -> None:
    (df
     .repartition(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(s3_result_uri))


if __name__ == "__main__":
    main(
        input_csv_files_uris=sys.argv[1],
        output_uri=sys.argv[2]
    )