# dags/ETL.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import sys

sys.path.append('./dag/ETL')  #  путь к скрипту


def run_generate_table():
    from ETL import result_in_EUR  # Импортирую функцию generate_table из вашего скрипта
    return result_in_EUR()



with DAG(
        dag_id="my_dag_for_ETL",
        start_date=datetime(2025, 1, 1),  
        schedule_interval="*/10 * * * *",  # Каждые 10 минут
        catchup=False
) as dag:
    generate_table_task = PythonOperator(
        task_id='my_table',
        python_callable=run_generate_table,
    )
