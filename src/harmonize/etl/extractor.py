import subprocess
import requests
import zipfile
import os
import json

class Extractor():
    def run_scrapy_spider(self):
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

    def descargar_descomprimir_zip(self, url, carpeta_destino='../../data/input'):
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
                archivos_a_borrar = self.descomprimir_archivo_csv(carpeta_destino, carpeta_temporal)

            os.remove('archivo_temporal.zip')
            # Borra los archivos ZIP después de descomprimirlos
            for archivo_a_borrar in archivos_a_borrar:
                os.remove(archivo_a_borrar)
        else:
            print('Error al descargar el archivo ZIP')

    def descomprimir_archivo_csv(self, carpeta_dest, carpeta_temp):
        # Función auxiliar que descompime y elimina archivos
        archivos_a_borrar = []  # Lista para almacenar archivos ZIP a borrar

        for root, dirs, files in os.walk(carpeta_dest):
            for nombre_archivo in files:
                if nombre_archivo.endswith('.zip'):
                    archivo_zip_path = os.path.join(root, nombre_archivo)
                    with zipfile.ZipFile(archivo_zip_path, 'r') as archivo_zip:
                        for archivo in archivo_zip.namelist():
                            if "CSV" in archivo:
                                # Supongamos que el archivo deseado está en la raíz del archivo ZIP
                                archivo_zip.extract(archivo, carpeta_dest+f"/{carpeta_temp}")
                                print(f'Descomprimido: {archivo}')
                    
                    # Agrega el archivo ZIP a la lista de archivos a borrar
                    archivos_a_borrar.append(archivo_zip_path)

        return archivos_a_borrar

    def extract(self):
        self.run_scrapy_spider()
        with open("enlaces_vivienda.json", 'r', encoding='utf-8') as file:
            # Cargar el contenido del archivo en un diccionario
            departamentos = json.load(file)
        
        # Imprimir el contenido del diccionario (opcional)
        for departamento, url in departamentos.items():
            print(f"{url}")
            # Descarga y descomprime el archivo ZIP principal
            self.descargar_descomprimir_zip(url[:-1])
            print(f"Datos {departamento}: ----- Descarga Finalizada")
            break

if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract()