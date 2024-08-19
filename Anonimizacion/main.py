from modules import DataConn, DataRetriever, load_data
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::MainModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

load_dotenv()

def main():  
    user_credentials = {
        "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
        "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
        "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
        "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
        "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME')
    }

    schema:str = "fedepr2345_coderhouse"
    table:str = "products"

    data_conn = DataConn(user_credentials, schema)
    data_retriever = DataRetriever()  

    try:
        df = data_retriever.anonymize_data()
        logging.info(f"Anonymized data retrieved with columns: {df.columns.tolist()}")  
        load_data(df)
        logging.info(f"Data uploaded to -> {schema}.{table}")
    except Exception as e:
        logging.error(f"Not able to upload data\n{e}")
    finally:
        data_conn.close_conn()

if __name__ == '__main__':  
    main()  