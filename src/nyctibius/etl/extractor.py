import subprocess
import requests
import zipfile
import os
import json
from dto.data_info import DataInfo
from scrapy.crawler import CrawlerProcess
from etl.standard_spider import StandardSpider  # Replace with your spider file's name



class Extractor():
    def __init__(self, url=None, depth=0, ext=['.csv','.xls','.xlsx','.zip']):
        self.url = url
        self.depth = depth
        self.ext = ext

    def run_mvp_spider(self):
        # Ejecuta una Spider que hace Scrapping de una pagina del DANE 
        # Retorna los links hacia la descarga de archivos .zip
        try:
            # Definición del comando
            command = ["scrapy", "runspider", "etl/mvp_spider.py"]

            # Ejecuta el comando para ejecutar la Spider
            subprocess.run(command, check=True)
            print(f"Successfully ran spider: MVP Spider")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while running the spider: {e}")

    def run_standard_spider(self):
        #searches for excel and csv files
        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0 (compatible; NictibiusBot/0.1)',
            'LOG_LEVEL': 'CRITICAL'
            # other Scrapy settings
        })
        process.crawl(StandardSpider, url=self.url, depth=self.depth, ext = self.ext)
        process.start()
        print(f"Successfully ran spider: Standard Spider")

    def descargar_descomprimir_mvpzip(self, url, carpeta_destino='../../data/input'):
        response = requests.get(url)
        carpeta_temporal = ""

        if response.status_code == 200:
            print("Archivo disponible, iniciando descarga...")
            # Guarda el contenido del archivo ZIP descargado en un archivo temporal
            with open('archivo_temporal.zip', 'wb') as archivo_temporal:
                archivo_temporal.write(response.content)

            # Descomprime el archivo ZIP
            with zipfile.ZipFile('archivo_temporal.zip', 'r') as archivo_zip:
                # Supongamos que el archivo deseado está en la raíz del archivo ZIP
                for archivo in archivo_zip.namelist():
                    carpeta_temporal = (archivo_zip.namelist())[0][:-1]
                    archivo_zip.extract(archivo, carpeta_destino)
                # Llama a la función para descomprimir archivos CSV
                archivos_a_borrar, list_datainfo_temp  = self.descomprimir_archivo_csv(carpeta_destino, carpeta_temporal, url)

            os.remove('archivo_temporal.zip')
            # Borra los archivos ZIP después de descomprimirlos
            for archivo_a_borrar in archivos_a_borrar:
                os.remove(archivo_a_borrar)
            return list_datainfo_temp
        else:
            print('Error al descargar el archivo ZIP')

    def descomprimir_archivo_csv(self, carpeta_dest, carpeta_temp, url_temp):
        # Función auxiliar que descompime y elimina archivos
        archivos_a_borrar = []  # Lista para almacenar archivos ZIP a borrar
        list_datainfo_temp = {}
        for root, dirs, files in os.walk(carpeta_dest):
            for nombre_archivo in files:
                if nombre_archivo.endswith('.zip'):
                    archivo_zip_path = os.path.join(root, nombre_archivo)
                    with zipfile.ZipFile(archivo_zip_path, 'r') as archivo_zip:
                        c = 1
                        for archivo in archivo_zip.namelist():
                            if "CSV" in archivo:
                                # Supongamos que el archivo deseado está en la raíz del archivo ZIP
                                archivo_zip.extract(archivo, carpeta_dest + f"/{carpeta_temp}")
                                print(f'Descomprimido: {archivo}')
                                list_datainfo_temp[f"datainfo_{carpeta_temp}_{c}"] = DataInfo(file_path=(carpeta_dest + "/"+ carpeta_temp + "/" + archivo ), url=url_temp, description=("datainfo_" +carpeta_temp + "_" + str(c)))
                                c+=1
                                
                    # Agrega el archivo ZIP a la lista de archivos a borrar
                    archivos_a_borrar.append(archivo_zip_path)

        return archivos_a_borrar, list_datainfo_temp

    def extract_mvp(self):
        with open("enlaces_vivienda.json", 'r', encoding='utf-8') as file:
            # Cargar el contenido del archivo en un diccionario
            departamentos = json.load(file)
        list_datainfo = {}
        # Imprimir el contenido del diccionario (opcional)
        for departamento, url in departamentos.items():
            print(f"{url}")
            # Descarga y descomprime el archivo ZIP principal
            list_datainfo_temp = self.descargar_descomprimir_mvpzip(url[:-1])
            list_datainfo = list_datainfo | list_datainfo_temp
            print(f"Datos {departamento}: ----- Descarga Finalizada")
            break
            
        os.remove("enlaces_vivienda.json")
        return list_datainfo
    
    def extract(self):
        list_datainfo = {}
        with open("Output_scrap.json", 'r', encoding='utf-8') as file:
            # Cargar el contenido del archivo en un diccionario
            links = json.load(file)

        download_dir = '../../data/input'
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        
        for filename, url in links.items():
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()

                # Save file to the directory
                filepath = os.path.join(download_dir, filename)
                with open(filepath, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            file.write(chunk)
                
                print(f"Downloaded '{filename}' from {url}")

            except requests.exceptions.RequestException as e:
                print(f"Error downloading {filename} from {url}: {e}")
        
            list_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filepath, url=url, description=("..."))

        os.remove("Output_scrap.json")
        return list_datainfo



if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract()
