from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import missingno as msno
import os
import requests
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import psycopg2
import smtplib

# Define default_args, DAG, and other parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monthly_stocks_etl',
    default_args=default_args,
    description='ETL DAG for Monthly Stocks Data',
    schedule_interval='@daily',
)

def enviar(**kwargs):
    try:
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()
        x.login('hashtagfp@gmail.com', 'lxuqcjhpcooexiph')  # Cambia tu contraseña !!!!!!!!
        body_text = f"high_volume_dates: {kwargs['ti'].xcom_pull(task_ids='etl_process', key='high_volume_dates')}"
        message = 'Subject: ETL Monthly high_volume_dates\n\n' + body_text
        x.sendmail('hashtagfp@gmail.com', 'ace.pinto17@gmail.com', message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

# Function to perform ETL
def run_etl_process(**kwargs):
    # Import libraries
    import pandas as pd
    import missingno as msno
    import os
    import requests
    from dotenv import load_dotenv
    from psycopg2.extras import execute_values

    # Load environment variables
    load_dotenv()

    # Connect to Redshift
    CODER_REDSHIFT_HOST = os.environ.get('CODER_REDSHIFT_HOST')
    CODER_REDSHIFT_DB = os.environ.get('CODER_REDSHIFT_DB')
    CODER_REDSHIFT_USER = os.environ.get('CODER_REDSHIFT_USER')
    CODER_REDSHIFT_PASS = os.environ.get('CODER_REDSHIFT_PASS')
    CODER_REDSHIFT_PORT = os.environ.get('CODER_REDSHIFT_PORT')

    try:
        conn = psycopg2.connect(
            host=CODER_REDSHIFT_HOST,
            dbname=CODER_REDSHIFT_DB,
            user=CODER_REDSHIFT_USER,
            password=CODER_REDSHIFT_PASS,
            port=CODER_REDSHIFT_PORT,
        )
        print("Connected to Redshift successfully!")

    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

    # API key for Alphavantage
    alphavantage_api_key = os.environ.get('ALPHAVANTAGE_API_KEY')

    # Function to get JSON from Alphavantage API
    def get_json(symbol):
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={symbol}&interval=5min&apikey={alphavantage_api_key}"
        r = requests.get(url)
        print(r)
        return r.json()

    # Function to format JSON and create DataFrame
    def format_json(json, symbol):
        # Se crea un DataFrame en Pandas a partir del json y se transpone para que las columnas sean los datos y las filas los días
        df = pd.DataFrame(json['Monthly Adjusted Time Series']).T
        # Se cambian los nombres de las columnas para que no tengan enumeración
        df.rename(columns=lambda x: x[3:], inplace=True)
        # Agregar columna index
        df['date'] = df.index
        # Se resetea el index para que date sea una columna y no un índice
        df.reset_index(drop=True, inplace=True)
        # Se definen los tipos de datos de las columnas ya que naturalmente son todos strings VARCHAR
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = pd.to_numeric(df['open'])
        df['high'] = pd.to_numeric(df['high'])
        df['low'] = pd.to_numeric(df['low'])
        df['close'] = pd.to_numeric(df['close'])
        df['adjusted close'] = pd.to_numeric(df['adjusted close'])
        # Cambiar volumen a millones ya que es un número muy grande y no se puede almacenar en un INT
        df['volume'] = pd.to_numeric(df['volume'])
        df['volume'] = round(df['volume'] / 1000000)
        df['volume'] = df['volume'].astype(int)
        df['dividend amount'] = pd.to_numeric(df['dividend amount'])
        #Se agrega la columna symbol con el símbolo de la acción
        df['symbol'] = symbol
        df['symbol'] = df['symbol'].astype(str)
        #Se devuelve el DataFrame ya transformado
        return df

    # Get data for different symbols
    symbols = ['AAPL', 'AMZN']
    dfs = []
    for symbol in symbols:
        data = get_json(symbol)
        df = format_json(data, symbol)
        dfs.append(df)

    # Concatenate DataFrames
    result_df = pd.concat(dfs, ignore_index=True)

    # Sort DataFrame
    result_df = result_df.sort_values(by=['date'], ascending=False)
    result_df = result_df.reset_index(drop=True)

    # Se revisa si existen filas de datos duplicadas
    result_df.duplicated().sum()

    # Revisamos visualmente la existencia de datos faltantes
    msno.matrix(result_df)

    # Revisamos los tipos de datos de cada columna para posteriormente crear la tabla en la base de datos con ellos
    result_df.info()

    # Se buscan las medidas de tendencia central y dispersión de las columnas numéricas para revisar que no haya datos atípicos
    result_df.describe()

    # Revisamos la cantidad de valores por símbolo
    # Se evidencia menos datos para Google
    result_df['symbol'].value_counts().sort_values(ascending=False)

    """# Creación de tabla en redshift (parte de la entrega 1)"""

    # Se utiliza la misma funcion de creacion de tabla de la entrega anterior
    def crear_tabla_redshift(nombre_tabla):
        try:
            # Se crea la tabla en Redshift con el nombre de la acción de la entrega anterior
            cursor = conn.cursor()
            # Se elimina la tabla si ya existe
            cursor.execute(f"DROP TABLE IF EXISTS {nombre_tabla};")
            # Se determinan los tipos de dato a partir de el .info anterior
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {nombre_tabla} ("open" FLOAT, "high" FLOAT, "low" FLOAT, "close" FLOAT, "adjusted close" FLOAT, "volume" INT, "dividend amount" FLOAT, "date" TIMESTAMP, "symbol" VARCHAR(255));
            """)
            conn.commit()
            cursor.close()
            print("Tabla creada exitosamente")
        except Exception as e:
            print("Error creating table")
            print(e)

    # Se crea la tabla en Redshift para cada todas las acciones de distintos Symbol
    crear_tabla_redshift('monthly_stocks_over_time')

    """# Carga de datos en la tabla de redshift"""

    def cargar_en_redshift(conn, table_name, dataframe):
        # Funcion para cargar un dataframe en una tabla de redshift, creando la tabla si no existe
        # Definir formato tipos de datos SQL
        dtypes = dataframe.dtypes
        cols = list(dtypes.index)
        print(cols)
        tipos = list(dtypes.values)
        type_map = {
            'float64': 'FLOAT',
            'int32': 'INT',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'VARCHAR(255)'
        }
        # Definir formato TIPO_DATO revisando el tipo de dato de cada columna del dataframe
        sql_dtypes = [type_map.get(str(dtype), 'VARCHAR(255)') for dtype in tipos]

        # Definir formato COLUMNA TIPO_DATO
        column_defs = [f'"{name}" {data_type}' for name, data_type in zip(cols, sql_dtypes)]

        # Combina las columnas y los tipos de datos en una sola cadena de SQL para crear la tabla con todas la columnas necesarias
        # En este caso del ejercicio creamos la tabla anteriormente por lo que no es necesario volver a crearla y mas adelante solo se insertan los datos.
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)});
            """
        print(table_schema)

        # Crear la tabla
        cur = conn.cursor()
        try:
            # Se ejecuta el comando para crear la tabla creado anteriormente
            cur.execute(table_schema)

            # Generar los valores a insertar
            values = [tuple(x) for x in dataframe.values]

            # Definir el INSERT con las columnas a insertar
            insert_sql = f"INSERT INTO {table_name} (\"open\", \"high\", \"low\", \"close\", \"adjusted close\", \"volume\", \"dividend amount\", \"date\", \"symbol\") VALUES %s"

            # Execute the transaction to insert the data
            cur.execute("BEGIN")
            execute_values(cur, insert_sql, values)
            cur.execute("COMMIT")
            print('Proceso terminado')
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()  # Rollback the transaction on error

        # Se carga el DataFrame en la tabla de Redshift
    cargar_en_redshift(conn=conn, table_name='monthly_stocks_over_time', dataframe=result_df)
    
    high_volume_dates = result_df[result_df['volume'] > 2500]['date'].dt.strftime('%Y-%m-%d').tolist()
    print(high_volume_dates)

    # Push high volume dates to XCom
    kwargs['ti'].xcom_push(key='high_volume_dates', value=high_volume_dates)

    # Filter result_df to include only rows with volume > 3000
    high_volume_df = result_df[result_df['volume'] > 2500]

    # Print or do something with the filtered DataFrame
    print("Rows with volume > 3000:")
    print(high_volume_df)

# Define the ETL task in the DAG
etl_task = PythonOperator(
    task_id='etl_process',
    python_callable=run_etl_process,
    provide_context=True,
    dag=dag,
)

email_task = PythonOperator(
    task_id='email_task',
    python_callable=enviar,
    provide_context=True,
    dag=dag,
)

# Set task dependencies (if needed)
# (e.g., etl_task >> another_task)
etl_task >> email_task

if __name__ == "__main__":
    dag.cli()

