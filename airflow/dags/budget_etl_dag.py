from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adicione o diretório src ao PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from extract import extract_data
from transform import transform_data
from load import load_data
from utils import load_config, setup_logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sao_paulo_budget_etl',
    default_args=default_args,
    description='ETL process for Sao Paulo budget data',
    schedule_interval=timedelta(days=1),
)

def etl_process():
    config = load_config()
    setup_logging(config.get('log_level', 'INFO'))
    
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data, config['database']['path'])

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

etl_task = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process,
    dag=dag,
)

# Define a ordem de execução das tarefas
extract_task >> transform_task >> load_task
etl_task