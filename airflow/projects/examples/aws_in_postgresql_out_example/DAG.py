import logging
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

DAG_name = 'aws_in_postgresql_out_example_dag'

LOG = logging.getLogger(__name__)


def prepare_input(**kwargs):
    ti = kwargs['ti']

    s3_bucket_name = Variable.get("s3_bucket_name") # dev -> bigdata-dev-bucket

    # AWS available .csv filenames (just for presentation):
    # 1. Electric_Production
    # 2. monthly-beer-production-in-austr
    src_data = "monthly-beer-production-in-austr"

    aws_input_uri = f's3a://{s3_bucket_name}/00_landing/{src_data}.csv'

    paths_list = [
        aws_input_uri
    ]
    csv_files_uris_str = ','.join(paths_list)
    LOG.info('csv_files_uris_str -> {}'.format(csv_files_uris_str))

    # output
    db_host = "postgres_db" # Variable.get("db_host")
    db_port = 5432 # Variable.get("db_port")
    db_name = "postgres_db" # Variable.get("db_name")
    postgresql_output_uri = f'jdbc:postgresql://{db_host}:{db_port}/{db_name}'

    # push info for next tasks
    ti.xcom_push(key='input_csv_files_uris', value=csv_files_uris_str)
    ti.xcom_push(key='postgresql_output_path', value=postgresql_output_uri)


@dag(
    dag_id=DAG_name,
    schedule_interval=None,  # Triggered manually
    max_active_runs=1,
    default_args={
        "owner": "mkrolczyk",
        "depends_on_past": True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=15),
    }
)
def process_aws_in_postgresql_out_example_dag():

    prepare_input_args_task = PythonOperator(
        task_id="prepare_input_args_task",
        provide_context=True,
        python_callable=prepare_input
    )

    run_spark_submit_task = SparkSubmitOperator(
        task_id="run_spark_submit_task",
        conn_id="spark-conn",
        py_files="spark/packages.zip",
        packages='org.apache.spark:spark-hadoop-cloud_2.12:3.4.0,org.postgresql:postgresql:42.6.0',
        application="spark/jobs/sample-project/sample-project-pipeline/python/aws_in_postgresql_out_example_job.py",
        application_args=[
            "{{ ti.xcom_pull(key='input_csv_files_uris') }}",
            "{{ ti.xcom_pull(key='postgresql_output_path') }}"
        ]
    )

    prepare_input_args_task >> run_spark_submit_task


main_dag = process_aws_in_postgresql_out_example_dag()
DAGS = [main_dag]