# Cotizaciones del dolar en 3 horarios

El script de python consulta a la API https://criptoya.com/api/dolar,trayendo las cotizaciones de los principales tipos de cambio(Oficial,Solidario,Blue,Ccb,Mep,entre otras) junto al valor y la fecha y hora en que se consultó la API.
Tiene programadas las consultas a la API en los horarios 7am,12pm y 17pm. Los datos se disponen en un Dataframe. Luego se crea una carpeta, donde dentro se genera un csv para almacenar los datos históricos,si ambos (directorio y csv) no existían; y si no, se agrega la información. También realicé la conexión con Redshift,utilizando config.ini (para no poner los datos en el archivo principal y en la próxima entrega quiero aplicarle algún tipo de seguridad) e inserté en la base de datos los mismos datos que estaba plasmando en el csv de históricos.

## Instalación
Utilicé python 3.11
Para instalar las librerias utilizadas en mi proyecto se debe realizar: pip install -r requirements.txt

## Instalación

Pasos para instalar y configurar el proyecto.

## Uso

Se debe ejecutar desde la consola con el siguiente comando: python main.py