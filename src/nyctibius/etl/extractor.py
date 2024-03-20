import os
import json
from ..dto.data_info import DataInfo
from tqdm import tqdm
import glob
from ..utils.extractor_utils import run_standard_spider, compressed2files, download_request
from itertools import islice

class Extractor():
    def __init__(self, path=str, url=str, depth=int, down_ext=list, download_dir=str):
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
            raise ValueError('Please use either path or URL mode, but not both simultaneously. \nIf both are needed, create two separate data instances and then merge them for processing.')
        elif not(path or url):
            raise ValueError('You must specify at least one of the following: a path or an URL.')
        elif url:
            self.mode = 0
        elif path:
            self.mode = 1


    def extract(self):
        # Set initial variables
        dict_datainfo = {}
        filepath = ""

        if self.mode == 0: # URL MODE
            extracted_extensions = set()
            # Run scraper and create a variable with the links on a temporal json with the extraction
            run_standard_spider(self.url, self.depth, self.down_ext)
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                links = json.load(file)
            tarea = False
            while not tarea:
                if len(links) > 30:
                    all = input(f"The provided link contains {len(links)} files. Would you like to download all of them? [Y/N]: ").strip().lower()
                    if "y" == all:
                        tarea = True
                    elif "n" == all:
                        tarea = True
                        files2download = int(input("Please enter the number of files you wish to download [Integer]: "))
                        assert files2download>0, "The number of files to download must be greater than 0." 
                        links = dict(islice(links.items(), files2download))
                    else:
                        pass
                else:
                    tarea = True
        
            assert links, "No files were found at the specified link. \nPlease verify the URL, search depth, and file extensions to ensure they are correct."

            # Set download folder
            if not os.path.exists(self.download_dir):
                os.makedirs(self.download_dir)
            # Scraper found links
            if links:
                # Iterate over the links to download files
                for filename, url in tqdm(links.items()):
                    # Request file download
                    filepath = download_request(url, filename, self.download_dir)                    
                    # Check if the file is a compressed archive to extract files and add them to the DataInfo dictionary 
                    extracted_files = []
                    extracted_extensions.add(filepath.split(".")[1])
                    if any(filepath.endswith(ext) for ext in self.compressed_ext):
                        extracted_files = list(compressed2files(filepath, self.download_dir, self.down_ext))
                        for extracted_file in extracted_files:
                            dict_datainfo[f"datainfo_{os.path.basename(extracted_file)}"] = DataInfo(file_path=extracted_file, url=url)
                    # If it is not a compressed archive, it is added to the DataInfo dictionary
                    else:
                        dict_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filepath, url=url)
                # Remove temporal extraction file
                os.remove("Output_scrap.json")
            # Scraper did not found links
            else:
                try:
                    # Request to download
                    filename = self.url.split("/")[-1]
                    filepath = download_request(self.url, filename, self.download_dir)
                    print(f"Successfully downloaded.")
                    dict_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filepath, url=self.url)
                # Exception if source are not available
                except Exception as e:
                    raise ValueError(f"Error downloading: \n{e}")
                
            assert dict_datainfo, (f"\nSuccessfully downloaded files with the following extensions: {extracted_extensions}. "
    "However, it appears there are no files matching your requested extensions: {self.down_ext} within any compressed files. "
    "Please ensure the requested file extensions are correct and present within the compressed files.")



        if self.mode == 1: # LOCAL MODE
            # Set variables to create DataInfo dictionary
            files_list = []
            compressed_list = []
            # Order extensions to process
            compressed_inter = set(self.compressed_ext) & set(self.down_ext)
            iter_ext = list(compressed_inter) + list(set(self.down_ext)-compressed_inter) 
            # Iter over ordered extensions to get all files of interest

            extracted_files = []

            for ext in iter_ext:
                full_pattern = os.path.join(self.path, f"*{ext}")
                if ext in self.compressed_ext:
                    compressed_list.extend(glob.glob(full_pattern))
                    for filepath in compressed_list:
                        extracted_files = list(compressed2files(input_archive=filepath, target_directory=self.download_dir, down_ext=self.down_ext))
                    files_list.extend(extracted_files)
                else:
                    files_list.extend(glob.glob(full_pattern))
            # Create DataInfos
            for filename in tqdm(files_list):
                try:
                    dict_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=filename, url="local")
                except Exception as e:
                        raise ValueError(f"Error: {e}")
                
            if not dict_datainfo:
                filename = self.path.split("/")[-1].split(".")[0]
                dict_datainfo[f"datainfo_{filename}"] = DataInfo(file_path=self.path, url="local")
        
        
        return dict_datainfo

    
if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract()
