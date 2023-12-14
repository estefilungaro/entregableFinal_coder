# Cotizaciones del dolar en 3 horarios

El script de python consulta a la API https://criptoya.com/api/dolar,trayendo las cotizaciones de los principales tipos de cambio(Oficial,Solidario,Blue,Ccb,Mep,entre otras) junto al valor y la fecha y hora en que se consultó la API.
Tiene programadas las consultas a la API en los horarios 7am,12pm y 17pm. Los datos se disponen en un Dataframe. Luego se crea una carpeta, donde dentro se genera un csv para almacenar los datos históricos,si ambos (directorio y csv) no existían; y si no, se agrega la información. También realicé la conexión con Redshift,utilizando config.ini (para no poner los datos en el archivo principal y en la próxima entrega quiero aplicarle algún tipo de seguridad) e inserté en la base de datos los mismos datos que estaba plasmando en el csv de históricos.

## Instalación
Utilicé python 3.11
Para instalar las librerias utilizadas en mi proyecto se debe realizar: pip install -r requirements.txt

Se debe crear en airflow desde admin -> connections,la conexión a redshift utilizando como nombre "coderhouse_redshift" y el Connection_type "postgres"
Crear una variable llamada "GMAIL_SECRET" con la clave de acceso al gmail para enviar el mail y en el código reemplazar el mail propio(en donde figure "e3885586@gmail.com").
## Uso

Se debe ejecutar desde la consola con el siguiente comando,teniendo docker arriba: docker compose up -d 

luego ingresar a airflow y en la seccion de DAG's, seleccionar el dag "dag_APItoRedshift" presionar play o automáticamente se ejecutará  a las 7am ,12 del mediodia y 17hs
