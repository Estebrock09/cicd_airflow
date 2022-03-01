from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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

    task_bq_op = BigQueryOperator(
        task_id="task_bq_op",
        sql=query_bq_op,
        use_legacy_sql=False,
        gcp_conn_id=GBQ_CONNECTION_ID,
        params={
            "google_project_id": "hallowed-hold-337921",
            "queryDataset": "estebrock_dataset",
            "queryTable": "time",
            "date_process": str(datetime.datetime.now().strftime("%Y-%m-%d")),
        },
    )

    t_end = DummyOperator(task_id="end")

    t_begin >> task_bq_op >> t_end
