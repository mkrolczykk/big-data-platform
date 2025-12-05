import os
import logging
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

DAG_name = 'trino_automated_action_example_dag'

LOG = logging.getLogger(__name__)

def prepare_sql(script_name):
    dag_path = os.path.dirname(__file__)
    SQL_QUERY_PATH = os.path.join(dag_path, f"sql/{script_name}")
    with open(SQL_QUERY_PATH, 'r') as f:
        sql_query = f.read()
        return sql_query


def send_congratulation_emails(**kwargs):
    # email sending logic
    print("emails has been sent!")


def trigger_top_employees_bonus_flow(**kwargs):
    # bonus process trigger logic
    print("Starting trigger top employees bonus flow process")

trino_aggr_query = prepare_sql("aggr_sales_data.sql")

@dag(
    dag_id=DAG_name,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=days_ago(1),
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
def trino_from_silver_to_gold_example_dag():

    aggr_sales_data_task = SQLExecuteQueryOperator(
        task_id="aggr_sales_data_task",
        conn_id="trino_default",
        sql=trino_aggr_query,
        params={
            "total_sales_floor_value": 1218115804,
            "bonus_limit": 10,
        },
        # handler=lambda cursor: [row for row in cursor.fetchall()],
        # return_last=True
    )

    send_congratulation_emails_task = PythonOperator(
        task_id="send_congratulation_emails_task",
        python_callable=send_congratulation_emails,
        provide_context=True
    )

    trigger_top_employees_bonus_flow_task = PythonOperator(
        task_id="trigger_top_employees_bonus_flow_task",
        python_callable=trigger_top_employees_bonus_flow,
        provide_context=True
    )

    aggr_sales_data_task >> [send_congratulation_emails_task, trigger_top_employees_bonus_flow_task]


main_dag = trino_from_silver_to_gold_example_dag()
DAGS = [main_dag]