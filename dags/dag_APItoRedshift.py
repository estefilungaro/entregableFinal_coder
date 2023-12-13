from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.main_corregido import *
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

# Obtener la conexión por ID
redshift_conn_id = "coderhouse_redshift_estefi"
hook = PostgresHook(postgres_conn_id=redshift_conn_id)


# Configuración del DAG
default_args = {
    'owner': 'estefania_coder',
    'start_date': datetime(2023, 11, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dag_APItoRedshift',
    default_args=default_args,
    schedule_interval="0 7,12,17 * * *",  # A las 7 am, 12 pm y 5 pm
    catchup=True,
)

# Tarea para Obtener Datos
obtener_datos_task = PythonOperator(
    task_id='obtener_datos_task',
    python_callable=obtener_datos,
    provide_context=True,
    dag=dag,
)
# enviar_email_task=PythonOperator(
#     task_id='smtp',
#     python_callable=enviar,
#     dag=dag
# )
# Configuración de Dependencias
obtener_datos_task
