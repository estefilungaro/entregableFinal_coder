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

# Obtener la contraseña de la conexión


# def crear_csv_si_no_existe():
#     if not os.path.exists('output_data'):
#         os.makedirs('output_data')

#     # Verificar si el archivo CSV existe, si no, crear uno vacío
#     if not os.path.exists('output_data/historico_dolares.csv'):
#         df_vacio = pd.DataFrame(columns=['Moneda', 'Valor'])
#         df_vacio.to_csv('output_data/historico_dolares.csv', index=False)

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

# crear_csv_task = PythonOperator(
#     task_id='crear_csv_task',
#     python_callable=crear_csv_si_no_existe,
#     provide_context=True,
#     dag=dag,
# )
# insertar_redshift_task = PythonOperator(
#     task_id='insertar_redshift_task',
#     python_callable=insertar_datos_en_redshift,
#     provide_context=True,
#     op_kwargs={
#         "df": "{{ task_instance.xcom_pull(task_ids='obtener_datos_task', key='return_value') }}"
#     },
#     dag=dag,
# )
# obtener_datos_task = PythonOperator(
#     task_id='obtener_datos_task',
#     python_callable=obtener_datos,
#     provide_context=True,
#     dag=dag,
# )
# Tarea para crear la tabla si no existe
# crear_tabla_task = PostgresOperator(
#     task_id="crear_tabla_task",
#     postgres_conn_id="coderhouse_redshift_estefi",
#     sql="""
#         CREATE TABLE IF NOT EXISTS historico_dolares_bd (
#             moneda VARCHAR(80),
#             valor FLOAT,
#             fecha TIMESTAMP distkey,
#             CONSTRAINT pk_fecha PRIMARY KEY (fecha)
#         ) SORTKEY(fecha);
#     """,
#     autocommit=True,
#     hook_params={	
#         "options": "-c search_path=estefanialevaungaro_coderhouse"
#     },
#     dag=dag,
# )
# dag = DAG (
#     dai_id='dag_smtp_envia_email',
#     schedule_interval=None,
#     on_success_callback=None,
#     catchup=False,
#     start_date=datetime(2023,11,10)
# )
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
