import os
import logging

import requests
import pandas as pd
from minio import Minio

baseUrl = 'http://servapibi.xm.com.co/hourly'
minioHost = 'minio:9000'


logger = logging.getLogger(__name__)
logging.basicConfig(encoding='utf-8', level=logging.INFO)

class Scraper:
    def __init__(self, date: str):
        self.date = date

    def _make_request(self) -> tuple[int, dict]:
        """
        Realiza la petición de la información para un dia en especifico
        
        Retorna:
        Tuple: El ststus code de la petición y el diccionario con la información solicitada
        """
        json_data = {
                        "MetricId": "DemaComeNoReg",
                        "StartDate": self.date,
                        "EndDate": self.date,
                        "Entity": "CIIU"
                    }
        response = requests.post(baseUrl, json=json_data)
        return response.status_code,response.json()

    def _struct_data(self, response: dict) -> str:
        """
        Filtra los datos de la respuesta de la API, los normaliza y realiza cálculos para obtener una estructura
        de datos con la suma de las horas de trabajo en actividades específicas.

        Parámetros:
        response (dict): El diccionario que devuelve la petición a la API de XM 
        
        Retorna:
        str: El lugar en el directorio /tmp donde se guardo el archivo para su posterior carga a MinIO
        """
        try:
            # Filtrar los elementos de 'Items' donde la actividad es 'INDUSTRIAS MANUFACTURERAS' y
            # la subactividad comienza con 'ELABORACIÓN DE PRODUCTOS' y se excluye lo que tiene que ver con tabaco
            lista = [
            item for item in response['Items']
            if item['HourlyEntities'][0]['Values']['Activity'] == 'INDUSTRIAS MANUFACTURERAS' and
                item['HourlyEntities'][0]['Values']['Subactivity'].startswith('ELABORACIÓN DE PRODUCTOS') and
                item['HourlyEntities'][0]['Values']['Subactivity'] != ('ELABORACIÓN DE PRODUCTOS DE TABACO')
            ] 

            # Normalizar los datos para convertir las listas anidadas en un DataFrame de pandas
            values = pd.json_normalize(lista, record_path=[['HourlyEntities']])

            # Quitar el prefijo Values de las columnas
            df = values.rename(columns = lambda x: x.strip('Values.'))

            # Crear una lista de las columnas que empiezan con la palabra Hour y ponerlas en formato float
            hours = [i for i in df.columns if i.startswith('Hour')]
            df[hours] = df[hours].astype(float)

            # Agregar en una nueva columna la suma de los 24 campos Hour
            df['sum']= df[list(df.filter(regex='Hour'))].sum(axis=1)

            # Excluir las columnas que comiencen con la palabra Hour, ya que las tenemos agregadas
            df = df.filter(regex=r'^(?!Hour).*', axis=1)

            # Agregar el campo fecha
            df['fecha'] = pd.to_datetime(self.date)

            file_path = f'/tmp/{self.date}.csv'

            df.to_csv(file_path, encoding='utf-8', index=False, header=True)
            
            return file_path
        
        except Exception as e:
            logger.warning("No se pudo guardar el archivo del dia {} debido a {}".format(self.date, e.__class__))
            return None
    
    def _save_minio(self, bucket: str, source_file: str):
        try:
            ACCESS_KEY = os.environ.get('MINIO_USER')
            SECRET_KEY = os.environ.get('MINIO_PASSWORD')

            client = Minio(
                            endpoint = minioHost, 
                            access_key = ACCESS_KEY, 
                            secret_key = SECRET_KEY, 
                            secure = False
                        )
            
            found = client.bucket_exists(bucket)
            if not found:
                client.make_bucket(bucket)
            
            destination_file = f"{self.date}.csv"
            client.fput_object(bucket, destination_file, source_file)

            return True
        
        except Exception as e:
            logger.warning("No se pudo guardar el archivo del dia {} debido a {}".format(self.date, e.__class__))
            return False

    
    
    def extract_data(self, bucket: str) -> list:
        try:
            resp = self._make_request()
            file = self._struct_data(resp[1])
            success = self._save_minio(bucket, file)

            results = {
                        'file': os.path.basename(file),
                        'local_path': file,
                        'minio_path': f'{bucket}/{self.date}',
                        'success': success
                      }
            
            return results
                
        except Exception as e:
            logger.warning("Error struct data to save in MinIO")
            raise ValueError(f"Error retrieving accounts: {str(e)}")

if __name__ == 'main':
    print("Extracción desde XM API")




    

