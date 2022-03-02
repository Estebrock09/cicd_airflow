from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import datetime

nameDAG = "airflow_bigquery_insert"
project = "hallowed-hold-337921"
owner = "estebrocktest"
email = ["estebrocktest@gmail.com"]
GBQ_CONNECTION_ID = "bigquery_default"

default_args = {
    "owner": owner,
    "depends_on_past": False,
    "start_date": datetime.datetime(2022, 1, 19),
    "email": email,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=1),
    "project_id": project,
}

query_bq_op = """
INSERT `{{ params.google_project_id }}.{{ params.queryDataset }}.{{ params.queryTable}}` (
    date
)
SELECT
    CURRENT_TIMESTAMP() as date
"""

with DAG(
    nameDAG,
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    schedule_interval="30 * * * *",
) as dag:
    t_begin = DummyOperator(task_id="begin")

    task_bq_op = BigQueryInsertJobOperator(
        task_id="task_bq_op",
        configuration={
            "query": {
                "query": query_bq_op,
                "useLegacySql": False,
            }
        },
    )

    t_end = DummyOperator(task_id="end")

    t_begin >> task_bq_op >> t_end

