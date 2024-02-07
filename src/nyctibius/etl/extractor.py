import logging
import requests
import os
import json
from ..dto.data_info import DataInfo
from scrapy.crawler import CrawlerProcess
from ..etl.standard_spider import StandardSpider  # Replace with your spider file's name
from tqdm import tqdm
import glob

import zipfile
import shutil
import tempfile
import tarfile
import py7zr


class Extractor():
    def __init__(self, path=None, url=None, depth=0, down_ext=['.csv','txt','.xls','.xlsx','.zip','.7z', '.tar', '.gz', '.tgz'], load_ext = ['.csv','.xls','.xlsx']):
        self.url = url
        self.depth = depth
        self.down_ext = down_ext
        self.path = path
        self.mode = -1
        self.load_ext = load_ext
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
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        process = CrawlerProcess({
            'LOG_LEVEL': 'CRITICAL',
            'LOG_ENABLED': False,
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
            # other Scrapy settings
        })
        process.crawl(StandardSpider, url=self.url, depth=self.depth, down_ext = self.down_ext)
        process.start()
        print(f"Successfully ran spider: Standard Spider")

    def extract(self):
        list_datainfo = {}
        filepath = ""
        if self.mode == 0:
            self.run_standard_spider()
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                links = json.load(file)

            download_dir = 'data/input'
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            for filename, url in tqdm(links.items()):
                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()

                    # Determine full filepath
                    filepath = os.path.join(download_dir, filename)
                    # Save file to the directory
                    with open(filepath, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:  # filter out keep-alive new chunks
                                file.write(chunk)

                    # Check if the file is a compressed archive
                    extracted_files = []
                    if any(filepath.endswith(ext) for ext in ['.zip', '.7z', '.tar', '.gz', '.tgz']):
                        extracted_files = self.extract_and_process_archive(filepath, download_dir)
                        for extracted_file in extracted_files:
                            list_datainfo[f"datainfo_{os.path.basename(extracted_file)}"] = DataInfo(file_path=extracted_file, url=url, description=("..."))
                    else:
                        list_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filepath, url=url, description=("..."))


                except requests.exceptions.RequestException as e:
                        return (f"Error downloading {filename} from {url}: {e}")

            os.remove("Output_scrap.json")

        if self.mode == 1:
            files_list = []
            for i in self.load_ext:
                full_pattern = os.path.join(self.path, f"*{i}")
                files_list.extend(glob.glob(full_pattern))
            for filename in tqdm(files_list):
                try:
                    list_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filename, url=None, description=("..."))
                except requests.exceptions.RequestException as e:
                        return (f"Error: {e}")
        return list_datainfo



    def extract_and_process_archive(self, input_archive, target_directory, current_depth=0, max_depth=4):
        found_files = []  # Initialize the list to keep track of found files
        if current_depth > max_depth:
            print(f"Reached max depth of {max_depth}. Stopping further extraction.")
            return found_files

        with tempfile.TemporaryDirectory() as temp_dir:
            # Determine the type of archive and extract accordingly
            if zipfile.is_zipfile(input_archive):
                with zipfile.ZipFile(input_archive, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
            elif tarfile.is_tarfile(input_archive):
                with tarfile.open(input_archive, 'r:*') as tar_ref:
                    tar_ref.extractall(temp_dir)
            elif input_archive.endswith('.7z'):
                with py7zr.SevenZipFile(input_archive, mode='r') as z_ref:
                    z_ref.extractall(temp_dir)
            else:
                print(f"Unsupported archive format: {input_archive}")
                return None

            # Process the extracted contents
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Check if file is a nested archive and if so, process it
                    if any(file_path.endswith(ext) for ext in ['.zip', '.7z', '.tar', '.gz', '.tgz']):
                        if current_depth < max_depth:
                            print(f"Extracting nested archive '{file}' at depth {current_depth + 1}")
                            found_files += self.extract_and_process_archive(file_path, target_directory, current_depth + 1, max_depth)
                    else:
                        print(f"Found file: {file} at depth {current_depth}")
                        destination_path = os.path.join(target_directory, os.path.basename(file_path))
                        shutil.move(file_path, destination_path)
                        found_files.append(destination_path)

        return found_files




if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract()
