import psycopg2
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DB = os.getenv('REDSHIFT_DB')

# Conexión a Redshift
def connect_redshift():
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT
        )
        print("Conexion a Redshift exitosa!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error en la conexion a Redshift: {e}")
        raise