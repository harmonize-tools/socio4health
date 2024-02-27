import logging
from scrapy.crawler import CrawlerProcess
from .standard_spider import StandardSpider
import zipfile
import shutil
import tempfile
import tarfile
import py7zr
import os


def run_standard_spider(url, depth, down_ext):
        #searches for excel and csv files
        logging.getLogger('scrapy').propagate = False
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        process = CrawlerProcess({
            'LOG_LEVEL': 'CRITICAL',
            'LOG_ENABLED': False,
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
            # other Scrapy settings
        })
        process.crawl(StandardSpider, url=url, depth=depth, down_ext = down_ext)
        process.start()
        print(f"Successfully ran spider: Standard Spider")

def compressed2files(input_archive, target_directory, down_ext, current_depth=0, max_depth=4, found_files=set()):
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
                            found_files |= set(compressed2files(file_path, target_directory, down_ext, current_depth + 1, max_depth, found_files))
                    elif  ("."+file.split(".")[1].lower()) in down_ext:
                        print(f"Found file: {file}")
                        destination_path = os.path.join(target_directory, os.path.basename(file_path))
                        shutil.move(file_path, destination_path)
                        found_files.add(destination_path)
        return found_files



