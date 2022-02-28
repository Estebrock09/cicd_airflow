from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import datetime

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG = 'bigquery_insert'
project = 'hallowed-hold-337921'
owner = 'estebrocktest'
email = ['estebrocktest@gmail.com']  # [email1, email2, email3]
GBQ_CONNECTION_ID = 'bigquery_default'
#######################################################################################

default_args = {
    'owner': owner,  # The owner of the task.
    'depends_on_past': False,  # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2022, 1, 19),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

query_bq_op = '''
INSERT `{{ params.google_project_id }}.{{ params.queryDataset }}.{{ params.queryTable}}` (
    date
)
SELECT
    CURRENT_TIMESTAMP() as date
'''

with DAG(nameDAG,
         default_args=default_args,
         catchup=False,  # Ver caso catchup = True
         max_active_runs=3,
         schedule_interval="30 * * * *") as dag:  # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################

    t_begin = DummyOperator(task_id="begin")

    task_bq_op = BigQueryOperator(task_id='task_bq_op',
                                  sql=query_bq_op,
                                  use_legacy_sql=False,
                                  bigquery_conn_id=GBQ_CONNECTION_ID,
                                  params={
                                      'google_project_id': "hallowed-hold-337921",
                                      'queryDataset': "estebrock_dataset",
                                      'queryTable': "time",
                                      'date_process': str(datetime.datetime.now().strftime("%Y-%m-%d"))
                                      # DEFAULT VALUE -> OJO: datetime.now() EN UTC +0
                                  }
                                  )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_bq_op >> t_end