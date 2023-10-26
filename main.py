import requests
import pandas as pd
import schedule
import time
import os
from datetime import datetime
#from redshift_connector import RedshiftConnector no me funciona
#from sqlalchemy import create_engine
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('config/config.ini')

# Tomo configs de Redshift desde config.ini
redshift_config = config['redshift']

host = redshift_config['host']
database = redshift_config['database']
user = redshift_config['user']
password = redshift_config['password']
port = redshift_config['port']

def insertar_datos_en_redshift(df):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    cursor = conn.cursor()

    for _,row in df.iterrows():
        moneda = row['Moneda']
        valor = row['Valor']
        fecha = row['Fecha y Hora']
        insert_query = "INSERT INTO historico_dolares_bd (moneda, valor, fecha) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (moneda, valor, fecha))

    conn.commit()
    cursor.close()
    conn.close()

def table_exists(cursor, table_name):
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    return cursor.fetchone()[0]

def crear_tabla():
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    cursor = conn.cursor()

    if not table_exists(cursor, 'historico_dolares_bd'):

        # ejecuto la query para crear la tabla en la db
        create_table_query = """
        CREATE TABLE historico_dolares_bd (
            moneda VARCHAR(80),
            valor FLOAT,
            fecha DATETIME,
            CONSTRAINT pk_fecha PRIMARY KEY (fecha)
        );
        """

        cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
# función para guardar el csv
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
#me traigo los datos de la API
def obtener_datos():
    try:
        url_dolar = 'https://criptoya.com/api/dolar'
        
        dolares = requests.get(url_dolar)
        
        if dolares.status_code == 200:
            dolares = dolares.json()
            dolares_filtrados = {key: value for key, value in dolares.items() if 'var' not in key and key != 'time'}

            df_dolares = build_df(dolares_filtrados)
            pd.set_option('display.float_format', lambda x: '%.2f' % x)
            print(df_dolares)
            
            # llamo a la función de guardado en csv
            guardar_historico_csv(df_dolares, 'output_data/historico_dolares.csv')
            crear_tabla()
            # inserto los datos en redshift
            insertar_datos_en_redshift(df_dolares)
        else:
            print(f"Error al obtener el recurso. Código de estado: {dolares.status_code}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == '__main__':
            
    #me fijo si la carpeta output_data existe, si no la creo
    if not os.path.exists('output_data'):
        os.makedirs('output_data')

    #si el archivo csv no existe lo creo y si existe le agrego los datos
    if not os.path.exists('output_data/historico_dolares.csv'):
        df_vacio = pd.DataFrame(columns=['Moneda', 'Valor'])
        df_vacio.to_csv('output_data/historico_dolares.csv', index=False)
            
    # hago que las requests a la API se realicen a las 7am, 12pm y 5pm
    schedule.every().day.at("7:00").do(obtener_datos)
    schedule.every().day.at("12:00").do(obtener_datos)
    schedule.every().day.at("17:00").do(obtener_datos)
   
    while True:
        schedule.run_pending()
        time.sleep(1)
        
