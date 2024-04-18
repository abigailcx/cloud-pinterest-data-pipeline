from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/abigailgcox@outlook.com/cloud_pinterest_0a55250cde99',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'abigailcox',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0a55250cde99_dag',
    # should be a datetime format
    start_date=datetime(2024, 3, 7),
    # check out possible intervals, should be a string - this will be a crontab string, can use '@daily'
    schedule_interval='0 18 * * *', 
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
