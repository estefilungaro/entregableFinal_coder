import requests
import pandas as pd
#import schedule
import time
import os
from datetime import datetime
import configparser
import psycopg2
from sqlalchemy import create_engine
import smtplib
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# Obtener la conexion por ID
redshift_conn_id = "coderhouse_redshift_estefi"
hook = PostgresHook(postgres_conn_id=redshift_conn_id)
def enviar(cotizacion_blue,**context):
    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        print(f"mi clave es: {Variable.get('GMAIL_SECRET')}")
        x.login('e3885586@gmail.com',
                Variable.get('GMAIL_SECRET'))
        subject = f'Reporte de Airflow sobre cotizacion del dolar'
        body_text = f'Valor del Dolar Blue: {cotizacion_blue:.2f}'
        message = 'Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('e3885586@gmail.com','e3885586@gmail.com',message)
        print('mail enviado!')
        print('Context:', context)
        print(subject)

    except Exception as exception:
        print(exception)
        print('Fallo envío mail')
#       except Exception as exception:
    # Capturar cualquier excepcion que pueda ocurrir durante el envío del correo electronico
        print(f'Error durante el envío del correo electronico: {exception}')
        print('Context:', context)
        print(subject)
        # Identificar el tipo de excepcion para proporcionar informacion más específica
        if isinstance(exception, smtplib.SMTPAuthenticationError):
            print('Error de autenticacion: Verifica las credenciales de Gmail en las variables de Airflow.')
        elif isinstance(exception, smtplib.SMTPException):
            print('Error SMTP: Verifica la configuracion del servidor SMTP y la conexion.')
        else:
            print('Fallo envío mail')

# Obtener las credenciales de la conexion
conn = hook.get_connection(redshift_conn_id)
host = conn.host
database = conn.schema
user = conn.login
password = conn.password
port = conn.port
def connect_to_redshift():
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    connection = engine.connect()
    return engine, connection


def insertar_datos_en_redshift(df):
    engine, connection = connect_to_redshift()

    nombre_tabla_redshift = 'historico_dolares_bd'
    df.rename(columns={'Fecha y Hora': 'fecha'}, inplace=True)
    # acá uso to_sql con method y if_exists para insertar varias filas
    df.to_sql(name=nombre_tabla_redshift, con=engine, if_exists='append', index=False, method='multi')

    # Cerramos la conexion después de usarla
    connection.close()

def table_exists(engine, table_name):
    with engine.connect() as connection:
        result = connection.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
        return result.fetchone()[0]

def crear_tabla():
    engine, connection = connect_to_redshift()

    if not table_exists(engine, 'historico_dolares_bd'):
        # Ejecuto la query para crear la tabla en la base de datos, quiero que la sort key sea la fecha ya que es la pk y 
        # distkey la fecha para que, como consulto a la API 3 veces al día me agrupe en cada nodo por este campo y así sea más simple analizar las fluctuaciones de las cotizaciones en cada día
        create_table_query = """
        CREATE TABLE historico_dolares_bd (
            moneda VARCHAR(80),
            valor FLOAT,
            fecha TIMESTAMP distkey,
            CONSTRAINT pk_fecha PRIMARY KEY (fecha)
        )SORTKEY(fecha);
        """

        with connection.begin() as trans:
            connection.execute(create_table_query)
            trans.commit()

    # Cerramos la conexion después de usarla
    connection.close()

# Funcion para guardar el CSV
# def guardar_historico_csv(df, nombre_archivo):
#     if os.path.exists(nombre_archivo):
#         df.to_csv(nombre_archivo, mode='a', header=False, index=False)
#     else:
#         df.to_csv(nombre_archivo, index=False)

def build_df(dolares):
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame(dolares.items(), columns=['Moneda', 'Valor'])
    df['Moneda'] = df['Moneda'].str.capitalize()
    # df['Fecha y Hora'] = formatted_date
    df['Fecha y Hora'] = pd.to_datetime(formatted_date)
    return df

def obtener_datos(**kwargs):
    try:
        url_dolar = 'https://criptoya.com/api/dolar'

        dolares = requests.get(url_dolar)

        if dolares.status_code == 200:
            dolares = dolares.json()
            dolares_filtrados = {key: value for key, value in dolares.items() if 'var' not in key and key != 'time'}

            df_dolares = build_df(dolares_filtrados)
            pd.set_option('display.float_format', lambda x: '%.2f' % x)
            print(df_dolares)

            # Llamo a la funcion de guardado en CSV
            #guardar_historico_csv(df_dolares, 'output_data/historico_dolares.csv')
            crear_tabla()
            # Inserto los datos en Redshift directamente desde aquí
            insertar_datos_en_redshift(df_dolares)
            cotizacion_blue =  df_dolares.loc[df_dolares['Moneda'] == 'Blue', 'Valor'].iloc[0]
            print(cotizacion_blue)
            if cotizacion_blue > 1000:
                enviar(cotizacion_blue)
            else:
                print("La cotizacion del dolr blue está por debajo de/igual $1000")
        else:
            print(f"Error al obtener el recurso. Codigo de estado: {dolares.status_code}")
    except Exception as e:
        print(f"Ocurrio un error: {e}")