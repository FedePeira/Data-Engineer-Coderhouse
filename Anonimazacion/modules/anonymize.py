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

    def retrieve_data(self):
        # Metodo para obtener los datos de la API
        try:
            response = requests.get(self.endpoint)  
            response.raise_for_status()  
            response_json = response.json() 

            data_df = pd.DataFrame(response_json)  
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

        # Obtener los datos desde el método específico  
        data_df = self.retrieve_data()  

        # Generar datos falsos  usando anonymizedf  
        an = anonymize(data_df) 

        fake_df = (  
            an  
            .fake_names("Titulo", chaining=True)  
            .fake_whole_numbers("Category", chaining=True)  
            .show_data_frame()  
        )  
        
        # Agregar fecha de ingesta  
        data_df['fecha_ingesta'] = datetime.now().date()

         # Aquí puedes seleccionar las Columnas que necesites  
        df_selected = data_df[['title', 'description', 'category', 'image', 'fecha_ingesta']]

        # Cambiar el nombre de las Columnas Seleccionadas  
        df_selected = df_selected.rename(columns={  
            'title': 'Titulo',  
            'description': 'Descripcion',  
            'category': 'Categoria',  
            'image': 'Imagen'  
        })  

        logging.info(f"Datos anonimizados: {df_selected.head()}")  
        return df_selected  