Se requiere de un archivo .env:
CODER_REDSHIFT_PASS = ""
CODER_REDSHIFT_USER = ""
CODER_REDSHIFT_HOST = ""
CODER_REDSHIFT_PORT = 5439
CODER_REDSHIFT_DB = ""
ALPHAVANTAGE_API_KEY = ""

Luego se debe ejecutar "docker-compose up" desde la carpeta donde se clono el repo
y luego desde Airflow activar el DAG monthly. Se adjunta un log y screenshots del ejemplo exitoso tanto en correo como en logs.
