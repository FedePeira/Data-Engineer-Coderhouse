from load_to_redshift import load_data  
from modules import DataRetriever
import logging

def main():  
    # Llamar a la función de anonimización
    data_retriever = DataRetriever()  

    # Mostrar el DataFrame anonimizado (si deseas) 
    print("----------------------------") 
    print('DataFrame selected:', data_retriever) 
    print("----------------------------")

    try:
        df = data_retriever.anonymize_data()
        print(df.head())
        print("----------------------------")
        print(df.dtypes) 
        # Cargar los datos a Redshift  
        load_data(df)
        logging.info(f"Data uploaded")
    except Exception as e:
        logging.error(f"Not able to upload data\n{e}")

if __name__ == '__main__':  
    main()  