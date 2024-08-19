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
        logging.info("DataRetriever initialized with endpoint: %s", self.endpoint)  

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
            data_df = self.retrieve_data()  

            data_df['fecha_ingesta'] = datetime.now().date()

            df_selected = data_df[['id', 'title', 'price', 'description', 'category', 'image', 'fecha_ingesta']]

            df_selected = df_selected.rename(columns={  
                'title': 'titulo', 
                'price': 'precio' ,
                'description': 'descripcion',
                'category': 'categoria',  
                'image': 'imagen',
            })  

            logging.info(f"Data anonymize: {df_selected.head()}")  
            return df_selected  

        except Exception as e:
            logging.error("An error occurred during data anonymization: %s", e)  
            raise