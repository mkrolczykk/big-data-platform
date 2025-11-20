import logging
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

DAG_name = 'process_minio_example_data_dag'

LOG = logging.getLogger(__name__)


def prepare_input(**kwargs):
    ti = kwargs['ti']

    s3_bucket_name = Variable.get("s3_bucket_name") # dev -> bigdata-dev-bucket
    output_base_uri = f's3a://{s3_bucket_name}/00_landing/daily-minimum-temperatures-in-me.csv'

    paths_list = [
        output_base_uri
    ]
    csv_files_uris_str = ','.join(paths_list)

    LOG.info('csv_files_uris_str -> {}'.format(csv_files_uris_str))

    ti.xcom_push(key='input_csv_files_uris', value=csv_files_uris_str)
    ti.xcom_push(key='s3_output_uri', value=f's3a://{s3_bucket_name}/01_bronze/daily_minimum_temperatures_in_me_enriched.csv')


@dag(
    dag_id=DAG_name,
    schedule_interval=None,  # Triggered manually
    max_active_runs=1,
    default_args={
        "owner": "mkrolczyk",
        "depends_on_past": True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=15),
    }
)
def process_minio_example_data_dag():

    prepare_input_args_task = PythonOperator(
        task_id="prepare_input_args_task",
        provide_context=True,
        python_callable=prepare_input
    )

    run_spark_submit_task = SparkSubmitOperator(
        task_id="run_spark_submit_task",
        conn_id="spark-conn",
        py_files="spark/packages.zip",
        packages='org.apache.spark:spark-hadoop-cloud_2.12:3.4.0',
        application="spark/jobs/sample-project/sample-project-pipeline/python/process_minio_example_data_job.py",
        application_args=[
            "{{ ti.xcom_pull(key='input_csv_files_uris') }}",
            "{{ ti.xcom_pull(key='s3_output_uri') }}"
        ]
    )

    prepare_input_args_task >> run_spark_submit_task


main_dag = process_minio_example_data_dag()
DAGS = [main_dag]