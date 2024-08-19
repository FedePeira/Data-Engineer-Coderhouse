from anonymizedf.anonymizedf import anonymize  
import os
from dotenv import load_dotenv
import requests
from io import StringIO
import pandas as pd
import logging
from datetime import datetime 

#Configuracion de logging para que aparezca .app.log
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::GetDataModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

# Cargar las Variables de Entorno
load_dotenv()  
API = os.getenv('API')

class DataRetriever:  
    def __init__(self) -> None:  
        self.endpoint: str = API  
        logging.info("DataRetriever inicializada con endpoint: %s", self.endpoint)  

    def retrieve_data(self):
        # Metodo para obtener los datos de la API
        logging.info("Intentado recuperar data de la API.")  
        try:
            response = requests.get(self.endpoint)  
            response.raise_for_status()  
            response_json = response.json() 

            data_df = pd.DataFrame(response_json)  
            logging.info('Data obtenida: %s', data_df.head())  
            print('--------------------------------')
            print('Anonymize DataFrane:', data_df)
            print('--------------------------------')
            return data_df
        except requests.exceptions.RequestException as e:  
            logging.error(f"Request error: {e}")  
            raise  
        except Exception as e:  
            logging.error(f"Not able to import the data from the API\n{e}")  
            raise 
    
    def anonymize_data(self):
        # Metodo para anonimizar los datos
        logging.info("Empezando a anonimizar los datos.")  

        try:
        # Obtener los datos desde el método específico  
            data_df = self.retrieve_data()  

            # Agregar fecha de ingesta  
            data_df['fecha_ingesta'] = datetime.now().date()

            # Aquí puedes seleccionar las Columnas que necesites  
            df_selected = data_df[['id', 'title', 'price', 'description', 'category', 'image', 'fecha_ingesta']]

            # Cambiar el nombre de las Columnas Seleccionadas  
            df_selected = df_selected.rename(columns={  
                'id': 'Id',
                'title': 'Titulo', 
                'price': 'Precio' ,
                'description': 'Descripcion',
                'category': 'Categoria',  
                'image': 'Imagen',
            })  

            logging.info(f"Datos anonimizados: {df_selected.head()}")  
            return df_selected  

        except Exception as e:
            logging.error("Un error ocurrio durante la anonimizacion de data: %s", e)  
            raise