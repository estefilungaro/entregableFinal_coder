import requests
import pandas as pd
#import schedule
import time
import os
from datetime import datetime
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('config/config.ini')

# Tomar configuraciones de Redshift desde config.ini
redshift_config = config['redshift']

host = redshift_config['host']
database = redshift_config['database']
user = redshift_config['user']
password = redshift_config['password']
port = redshift_config['port']
##uso sslmode para que la conexión a la base de datos sea segura

def connect_to_redshift():
    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        sslmode='require'
    )

def insertar_datos_en_redshift(df):
    conn = connect_to_redshift()
    cursor = conn.cursor()
    
    nombre_tabla_redshift = 'historico_dolares_bd'

    # acá uso to_sql con method y if_exists para insertar varias filas
    df.to_sql(name=nombre_tabla_redshift, con=cursor, if_exists='append', index=False, method='multi')

def table_exists(cursor, table_name):
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    return cursor.fetchone()[0]

def crear_tabla():
    conn = connect_to_redshift()
    cursor = conn.cursor()

    if not table_exists(cursor, 'historico_dolares_bd'):
        # Ejecuto la query para crear la tabla en la base de datos,quiero que la sort key sea la fecha ya que es la pk y 
        # distkey la fecha para que,como consulto a la API 3 veces al día me agrupe en cada nodo por este campo y así sea más simple analizar las fluctuaciones de las cotizaciones en cada día
        create_table_query = """
        CREATE TABLE historico_dolares_bd (
            moneda VARCHAR(80),
            valor FLOAT,
            fecha TIMESTAMP distkey,
            CONSTRAINT pk_fecha PRIMARY KEY (fecha)
        )SORTKEY(fecha);
        """

        cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Función para guardar el CSV
def guardar_historico_csv(df, nombre_archivo):
    if os.path.exists(nombre_archivo):
        df.to_csv(nombre_archivo, mode='a', header=False, index=False)
    else:
        df.to_csv(nombre_archivo, index=False)

def build_df(dolares):
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame(dolares.items(), columns=['Moneda', 'Valor'])
    df['Moneda'] = df['Moneda'].str.capitalize()
    df['Fecha y Hora'] = formatted_date
    return df

# Obtener datos de la API
# def obtener_datos():
#     try:
#         url_dolar = 'https://criptoya.com/api/dolar'

#         dolares = requests.get(url_dolar)

#         if dolares.status_code == 200:
#             dolares = dolares.json()
#             dolares_filtrados = {key: value for key, value in dolares.items() if 'var' not in key and key != 'time'}

#             df_dolares = build_df(dolares_filtrados)
#             pd.set_option('display.float_format', lambda x: '%.2f' % x)
#             print(df_dolares)

#             # Llamo a la función de guardado en CSV
#             guardar_historico_csv(df_dolares, 'output_data/historico_dolares.csv')
#             crear_tabla()
#             # Inserto los datos en Redshift
#             insertar_datos_en_redshift(df_dolares)
#         else:
#             print(f"Error al obtener el recurso. Código de estado: {dolares.status_code}")
#     except Exception as e:
#         print(f"Ocurrió un error: {e}")
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

            # Llamo a la función de guardado en CSV
            guardar_historico_csv(df_dolares, 'output_data/historico_dolares.csv')
            crear_tabla()
            # Inserto los datos en Redshift directamente desde aquí
            insertar_datos_en_redshift(df_dolares)
        else:
            print(f"Error al obtener el recurso. Código de estado: {dolares.status_code}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")


if __name__ == '__main__':
    # Me fijo si la carpeta output_data existe, si no la creo
    if not os.path.exists('output_data'):
        os.makedirs('output_data')

    # Si el archivo CSV no existe, lo creo y si existe le agrego los datos
    if not os.path.exists('output_data/historico_dolares.csv'):
        df_vacio = pd.DataFrame(columns=['Moneda', 'Valor'])
        df_vacio.to_csv('output_data/historico_dolares.csv', index=False)

    # Hago que las requests a la API se realicen a las 7am, 12pm y 5pm
    # schedule.every().day.at("07:00").do(obtener_datos)
    # schedule.every().day.at("12:00").do(obtener_datos)
    # schedule.every().day.at("17:00").do(obtener_datos)

    while True:
        #schedule.run_pending()
        time.sleep(1)
