Se requiere de un archivo .env:
CODER_REDSHIFT_PASS = ""
CODER_REDSHIFT_USER = ""
CODER_REDSHIFT_HOST = ""
CODER_REDSHIFT_PORT = 5439
CODER_REDSHIFT_DB = ""
ALPHAVANTAGE_API_KEY = ""

Luego se debe ejecutar "docker-compose up" desde la carpeta donde se clono el repo
y luego desde Airflow activar el DAG monthly. Se adjunta un log y screenshots del ejemplo exitoso tanto de correo como logs.

El correo de alerta enviado se realiza cuando se encuentran valores de volumenes mas altos de lo normal (>2500) dentro de las acciones obtenidas.

Un detalle es que la API solo soporta 20 llamadas diarias por lo que si se prueba de forma reiterada recomiendo usar una vpn para cambiar la ip desde la cual se consulta.
