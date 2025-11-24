import os
import logging
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

DAG_name = 'trino_processing_example_dag'

LOG = logging.getLogger(__name__)

dag_path = os.path.dirname(__file__)
SQL_QUERY_PATH = os.path.join(dag_path, "sql/get_car_sales_data.sql")
with open(SQL_QUERY_PATH, 'r') as f:
    car_sales_sql_query = f.read()

def log_trino_results(**kwargs):
    ti = kwargs['ti']
    catalogs = ti.xcom_pull(task_ids='show_catalogs_task')
    sales_data = ti.xcom_pull(task_ids='get_car_sales_data_task')

    LOG.info("=== TRINO CATALOGS ===")
    if catalogs:
        for catalog in catalogs:
            LOG.info(catalog)

    LOG.info("=== SALES DATA ===")
    if sales_data:
        for row in sales_data:
            LOG.info(row)


@dag(
    dag_id=DAG_name,
    schedule_interval=None,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "mkrolczyk",
        "depends_on_past": False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5),
    },
    tags=["trino"]
)
def trino_processing_example_dag():

    show_catalogs_task = SQLExecuteQueryOperator(
        task_id="show_catalogs_task",
        conn_id="trino_default",
        sql="SHOW CATALOGS",
        handler=lambda cursor: [row[0] for row in cursor.fetchall()],
        return_last=True
    )

    get_car_sales_data_task = SQLExecuteQueryOperator(
        task_id="get_car_sales_data_task",
        conn_id="trino_default",
        sql=car_sales_sql_query,
        params={
            "country": "PL",
            "query_limit": 10,
        },
        handler=lambda cursor: [row for row in cursor.fetchall()],
        return_last=True
    )

    log_results_task = PythonOperator(
        task_id="log_results_task",
        python_callable=log_trino_results,
        provide_context=True
    )

    show_catalogs_task >> get_car_sales_data_task >> log_results_task


main_dag = trino_processing_example_dag()
DAGS = [main_dag]