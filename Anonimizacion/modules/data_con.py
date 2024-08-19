import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv  
import os

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::DataConnectionModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

load_dotenv()  

class DataConn:
    def __init__(self, config: dict,schema: str):
        self.config = config
        self.schema = schema
        self.db_engine = None
        logging.info("DataConn initialize with the schema: %s", self.schema)  

    def get_conn(self):
        username = self.config.get('REDSHIFT_USERNAME')
        password = self.config.get('REDSHIFT_PASSWORD')
        host = self.config.get('REDSHIFT_HOST')
        port = self.config.get('REDSHIFT_PORT', '5439')
        dbname = self.config.get('REDSHIFT_DBNAME')

        connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
        self.db_engine = create_engine(connection_url)
        logging.info(f"Connecting to Redshift at {host}:{port} as {username}")  

        try:
            with self.db_engine.connect() as connection:
                result = connection.execute('SELECT 1;')
                logging.info("Connection created")  
            if result:
                logging.info("Connection verified successfully.")  
                return self.db_engine
        except Exception as e:
            logging.error(f"Failed to create connection: {e}")
            raise RuntimeError("Failed to create a connection to Redshift.")  


    def close_conn(self):
        if self.db_engine:
            self.db_engine.dispose()
            logging.info("Connection to Redshift closed.")
        else:
            logging.warning("No active connection to close.")

def load_data(df):  
    if df is None or df.empty:  
        logging.warning("No data to load, df empty.")  
        print("No data to load, df empty.")  
        return  
    
    # Validar y recortar longitudes de las columnas  
    df['Titulo'] = df['Titulo'].str.slice(0, 255)  
    df['Descripcion'] = df['Descripcion'].str.slice(0, 255)  
    df['Categoria'] = df['Categoria'].str.slice(0, 255)  
    df['Imagen'] = df['Imagen'].str.slice(0, 255)  
    df = df.fillna('')

    # Crear un diccionario de configuración  
    config = {  
        'REDSHIFT_USERNAME': os.getenv('REDSHIFT_USERNAME'),  
        'REDSHIFT_PASSWORD': os.getenv('REDSHIFT_PASSWORD'),  
        'REDSHIFT_HOST': os.getenv('REDSHIFT_HOST'),  
        'REDSHIFT_PORT': os.getenv('REDSHIFT_PORT', '5439'),  
        'REDSHIFT_DBNAME': os.getenv('REDSHIFT_DBNAME')  
    }  

    logging.info(f"Configuración de conexión a Redshift: {config}")  
    
    # Crear una instancia de DataConn  
    data_conn = DataConn(config=config, schema='public')  

    try:  
        db_engine = data_conn.get_conn()  
        if data_conn.db_engine is None:  
            raise Exception("No se pudo establecer la conexión.") 
    except Exception as e:  
        logging.error("Error estableeciendo una conexion con la base ed datos: %s", e)  
        print(f"Error estableciendo conexión a la base de datos: {e}")  
        return  

    try:  
        # Usar el motor para operar  
        with db_engine.connect() as connection:  
            # Crear la tabla en Redshift si no existe  
            connection.execute(f"""  
            CREATE TABLE IF NOT EXISTS {data_conn.schema}.productos (  
                id INTEGER PRIMARY KEY,  
                title VARCHAR(255), 
                price DECIMAL(10, 2),  
                description VARCHAR(255), 
                category VARCHAR(255),  
                image VARCHAR(255), 
                fecha_ingesta DATE  
            );  
            """)  
            print("Tabla en Redshift lista!")  

            # Preparar datos para inserción en bloque  
            block_size = 20   
            for start in range(0, len(df), block_size):  
                end = start + block_size  
                block_df = df.iloc[start:end]  

                if block_df.empty:
                    print("block_df is empty")

                rows_to_insert = [tuple(row) for row in block_df.to_numpy()]
                    
                insert_query = f"""  
                INSERT INTO  {data_conn.schema}.productos (title, price, description, category, image, fecha_ingesta)  
                VALUES (%s, %s, %s, %s, %s, %s)
                """  
                
                connection.execute(insert_query, rows_to_insert)  
                              
                print(f"Se ha agregado un bloque de {len(rows_to_insert)} registros a Redshift.")  

    except Exception as e:  
        logging.error("Error cargando datos a Redshift: %s", e)  
        print(f"Error cargando datos a Redshift: {e}")  
    finally:  
        data_conn.close_conn()  
