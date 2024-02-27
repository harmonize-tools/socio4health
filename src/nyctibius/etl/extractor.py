import logging
import requests
import os
import json
from ..dto.data_info import DataInfo
from tqdm import tqdm
import glob
from ..utils.extractor_utils import run_standard_spider, compressed2files


class Extractor():
    def __init__(self, path=None, url=None, depth=0, down_ext=['.csv','.xls','.xlsx', ".txt", ".sav", ".zip"], download_dir="data/input"):
        # Set variables for online scrap
        self.compressed_ext = ['.zip','.7z', '.tar', '.gz', '.tgz']
        self.url = url
        self.depth = depth
        self.down_ext = down_ext
        self.download_dir = download_dir
        # Set variables for local files
        self.path = path
        # Set mode
        self.mode = -1
        if path and url:
            raise ValueError(f'Use only path or url mode. If you need to use both, create 2 datainfos and merge it to the transformer')
        elif not(path or url):
            raise ValueError(f'Use at least a path or an url.')
        elif url:
            self.mode = 0
        elif path:
            self.mode = 1



    def extract(self):
        # Set initial variables
        dict_datainfo = {}
        filepath = ""

        if self.mode == 0: # URL MODE
            # Run scraper and create a variable with the links on a temporal json with the extraction
            run_standard_spider(self.url, self.depth, self.down_ext)
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                links = json.load(file)
            # Set download folder
            
            if not os.path.exists(self.download_dir):
                os.makedirs(self.download_dir)
            # Scraper found links
            if links:
                # Iterate over the links to download files
                for filename, url in tqdm(links.items()):
                    try:
                        # Request to download
                        response = requests.get(url, stream=True)
                        response.raise_for_status()
                        filepath = os.path.join(self.download_dir, filename)
                        # Save file to the directory
                        with open(filepath, 'wb') as file:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    file.write(chunk)

                        # Check if the file is a compressed archive to extract files and add them to the DataInfo dictionary 
                        extracted_files = []
                        if any(filepath.endswith(ext) for ext in self.compressed_ext):
                            extracted_files = list(compressed2files(filepath, self.download_dir, self.down_ext))
                            for extracted_file in extracted_files:
                                dict_datainfo[f"datainfo_{os.path.basename(extracted_file)}"] = DataInfo(file_path=extracted_file, url=url, description=("..."))
                        # If it is not a compressed archive, it is added to the DataInfo dictionary
                        else:
                            dict_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filepath, url=url, description=("..."))

                    # Exception if source are not available
                    except requests.exceptions.RequestException as e:
                            return (f"Error downloading {filename} from {url}: {e}")
                # Remove temporal extraction file
                os.remove("Output_scrap.json")
            # Scraper did not found links
            else:
                try:
                    # Request to download
                    filename = self.url.split("/")[-1]
                    response = requests.get(self.url, stream=True)
                    response.raise_for_status()
                    filepath = os.path.join(self.download_dir, filename)
                    # Save file to the directory
                    with open(filepath, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                file.write(chunk)
                # Exception if source are not available
                except requests.exceptions.RequestException as e:
                    return (f"Error downloading {filename} from {url}: {e}")
            

        if self.mode == 1: # LOCAL MODE
            # Set variables to create DataInfo dictionary
            files_list = []
            compressed_list = []
            # Order extensions to process
            compressed_inter = set(self.compressed_ext) & set(self.down_ext)
            iter_ext = list(compressed_inter) + list(set(self.down_ext)-compressed_inter) 
            # Iter over ordered extensions
            for i in iter_ext:
                full_pattern = os.path.join(self.path, f"*{i}")
                # If a compressed file
                if i in self.compressed_ext:
                    compressed_list.extend(glob.glob(full_pattern))
                    for filepath in compressed_list:
                        extracted_files = list(compressed2files(input_archive=filepath, target_directory=self.download_dir, down_ext=self.down_ext))
                    files_list.extend(extracted_files)
                else:
                    files_list.extend(glob.glob(full_pattern))
            for filename in tqdm(files_list):
                try:
                    dict_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filename, url=None, description=("..."))
                except requests.exceptions.RequestException as e:
                        return (f"Error: {e}")
                
        return dict_datainfo

    
if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract()
