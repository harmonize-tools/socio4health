import logging
import requests
import os
import json
from ..dto.data_info import DataInfo
from scrapy.crawler import CrawlerProcess
from ..etl.standard_spider import StandardSpider  # Replace with your spider file's name
from tqdm import tqdm
import glob


class Extractor():
    def __init__(self, path=None, url=None, depth=0, ext=['.csv','.xls','.xlsx','.zip']):
        self.url = url
        self.depth = depth
        self.ext = ext
        self.path = path
        self.mode = 0
        if path and url:
            print("Solo una de las 2 pls")
        elif not(path or url):
            print("Al menos alguno de los 2 pls")
        elif url:
            self.mode = 0
        elif path:
            self.mode = 1



    def run_standard_spider(self):
        #searches for excel and csv files
        logging.getLogger('scrapy').propagate = False
        logging.getLogger('urllib3').setLevel(logging.ERROR)
        process = CrawlerProcess({
            'LOG_LEVEL': 'CRITICAL',
            'LOG_ENABLED': False,
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
            # other Scrapy settings
        })
        process.crawl(StandardSpider, url=self.url, depth=self.depth, ext = self.ext)
        process.start()
        print(f"Successfully ran spider: Standard Spider")

    def extract(self):
        list_datainfo = {}
        filepath = ""
        if self.mode == 0:
            self.run_standard_spider()
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                # Cargar el contenido del archivo en un diccionario
                links = json.load(file)

            download_dir = 'data/input'
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            print("Extracting...")
            for filename, url in tqdm(links.items()):
                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()

                    # Save file to the directory
                    filepath = os.path.join(download_dir, filename)
                    with open(filepath, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:  # filter out keep-alive new chunks
                                file.write(chunk)
                    
                    #print(f"Downloaded '{filename}' from {url}")

                except requests.exceptions.RequestException as e:
                        return (f"Error downloading {filename} from {url}: {e}")
            
                list_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filepath, url=url, description=("..."))
                

            os.remove("Output_scrap.json")
        if self.mode == 1:
            files_list = []
            for i in self.ext:
                full_pattern = os.path.join(self.path, f"*{i}")
                files_list.append(glob.glob(full_pattern))
            for filename in files_list:
                try:
                    list_datainfo[f"datainfo_{filename[0]}"] = DataInfo(file_path=filename[0], url=None, description=("..."))
                except requests.exceptions.RequestException as e:
                        return (f"Error: {e}")
        return list_datainfo



if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract()
