import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)
dag_config = Variable.get("pingservice", deserialize_json=True)

def call_rest_api():
    url = dag_config["ping_url"]
    response = requests.get(url)
    logger.info(response)
    if response.status_code == 200:
        logger.info('success' )
    else:
        logger.error(response.status_code)
        
default_args = {
                'owner': 'airflow',
                'depends_on_past': False,
                'start_date': days_ago(1),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }
dag = DAG(
            'pingservice_dag3',
            default_args=default_args,
            description='A simple DAG to call a REST API',
            schedule_interval= dag_config["schedule"] ,
        )
call_api_task = PythonOperator(
            task_id='call_rest_api3',
            python_callable=call_rest_api,
            dag=dag,
        )
start = DummyOperator(task_id="start3",)

end = DummyOperator(task_id="end3",)
    
start >> call_api_task >> end
