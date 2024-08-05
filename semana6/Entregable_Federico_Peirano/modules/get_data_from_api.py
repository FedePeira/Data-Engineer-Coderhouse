import requests
from io import StringIO
import pandas as pd
import logging
from datetime import datetime 

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::GetDataModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

class DataRetriever:  
    def __init__(self) -> None:  
        self.endpoint: str = "https://fakestoreapi.com/products"  
    
    def get_data(self):  
        try:  
            response = requests.get(self.endpoint)  
            response.raise_for_status()  
            response_json = response.json() 

            data_df = pd.DataFrame(response_json)  

            data_df['fecha_ingesta'] = datetime.now().date()  

            logging.info(f"Data created: {data_df.head()}")  
            return data_df  

        except requests.exceptions.RequestException as e:  
            logging.error(f"Request error: {e}")  
            raise  
        except Exception as e:  
            logging.error(f"Not able to import the data from the API\n{e}")  
            raise 