import os
import json
import pandas as pd
from tqdm import tqdm
import glob
from socio4health.utils.extractor_utils import run_standard_spider, compressed2files, download_request
from itertools import islice
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Extractor():
    def __init__(self, path: str, url: str, depth: int, down_ext: list, download_dir: str, key_words: list):
        # Initialize variables for online scraping
        self.compressed_ext = ['.zip', '.7z', '.tar', '.gz', '.tgz']
        self.url = url
        self.depth = depth
        self.down_ext = down_ext
        self.download_dir = download_dir
        self.key_words = key_words
        self.path = path
        self.mode = -1
        self.dataframes = []

        if path and url:
            logging.error('Both path and URL cannot be specified. Please choose one.')
            raise ValueError(
                'Please use either path or URL mode, but not both simultaneously. '
                'If both are needed, create two separate data instances and then merge them for processing.')
        elif not (path or url):
            logging.error('Neither path nor URL was specified.')
            raise ValueError('You must specify at least one of the following: a path or an URL.')
        elif url:
            self.mode = 0
        elif path:
            self.mode = 1

    def extract(self):
        logging.info("----------------------")
        logging.info("Starting data extraction...")
        try:
            if self.mode == 0:
                self._extract_online_mode()
            elif self.mode == 1:
                self._extract_local_mode()
            logging.info("Extraction completed successfully.")
        except Exception as e:
            logging.error(f"Exception while extracting data: {e}")
            raise ValueError(f"Extraction failed: {str(e)}")

        return self.dataframes

    def _extract_online_mode(self):
        logging.info("Extracting data in online mode...")
        extracted_extensions = set()

        # Run scraper and create a temporary JSON with the extraction links
        run_standard_spider(self.url, self.depth, self.down_ext, self.key_words)

        try:
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                links = json.load(file)
        except Exception as e:
            logging.error(f"Failed to read links from Output_scrap.json: {e}")
            raise

        tarea = False
        while not tarea:
            if len(links) > 30:
                all = input(
                    f"The provided link contains {len(links)} files. Would you like to download all of them? [Y/N]: ").strip().lower()
                if all == "y":
                    tarea = True
                elif all == "n":
                    tarea = True
                    files2download = int(input("Please enter the number of files you wish to download [Integer]: "))
                    assert files2download > 0, "The number of files to download must be greater than 0."
                    links = dict(islice(links.items(), files2download))
            else:
                tarea = True

        # Set download folder
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            logging.info(f"Created download directory: {self.download_dir}")

        # Scraper found links
        if links:
            # Iterate over the links to download files
            for filename, url in tqdm(links.items()):
                # Request file download
                filepath = download_request(url, filename, self.download_dir)
                # Check if the file is a compressed archive to extract files and add them to the DataFrame list
                extracted_files = []
                extracted_extensions.add(filepath.split(".")[-1])
                if any(filepath.endswith(ext) for ext in self.compressed_ext):
                    logging.info(f"Extracting files from compressed archive: {filepath}")
                    extracted_files = list(compressed2files(filepath, self.download_dir, self.down_ext))
                    for extracted_file in extracted_files:
                        self.dataframes.append(pd.read_csv(extracted_file))
                    os.remove(filepath)
                    logging.info(f"Removed compressed file after extraction: {filepath}")
                else:
                    self.dataframes.append(pd.read_csv(filepath))
                    logging.info(f"Downloaded file: {filename}")

        else:
            # Handle case where no links were found
            try:
                filename = self.url.split("/")[-1]
                if len(filename.split(".")) == 1:
                    filename += ".zip"
                filepath = download_request(self.url, filename, self.download_dir)
                logging.info(f"Successfully downloaded {filename} file.")

                if any(filepath.endswith(ext) for ext in self.compressed_ext):
                    logging.info(f"{filename} contains compressed files, extracting...")
                    extracted_files = list(compressed2files(filepath, self.download_dir, self.down_ext))
                    for extracted_file in extracted_files:
                        self.dataframes.append(pd.read_csv(extracted_file))
                    try:
                        os.remove(filepath)
                        logging.info(f"Removed compressed file: {filepath}")
                    except:
                        logging.warning(f"Could not remove compressed file: {filepath}")

            except Exception as e:
                logging.error(f"Error downloading: {e}")
                raise ValueError(
                    f"No files were found at the specified link. Please verify the URL, search depth, and file extensions.")

        os.remove("Output_scrap.json")
        assert self.dataframes, (
            f"\nSuccessfully downloaded files with the following extensions: {extracted_extensions}. "
            "However, it appears there are no files matching your requested extensions: {self.down_ext} within any compressed files. "
            "Please ensure the requested file extensions are correct and present within the compressed files.")

    def _extract_local_mode(self):
        logging.info("Extracting data in local mode...")
        # Set variables to create DataFrame list
        files_list = []
        compressed_list = []

        # Order extensions to process
        compressed_inter = set(self.compressed_ext) & set(self.down_ext)
        iter_ext = list(compressed_inter) + list(set(self.down_ext) - compressed_inter)

        extracted_files = []

        for ext in iter_ext:
            full_pattern = os.path.join(self.path, f"*{ext}")
            if ext in self.compressed_ext:
                compressed_list.extend(glob.glob(full_pattern))
                for filepath in compressed_list:
                    extracted_files.extend(
                        compressed2files(input_archive=filepath, target_directory=self.download_dir,
                                         down_ext=self.down_ext))
            else:
                files_list.extend(glob.glob(full_pattern))

        # Create DataFrames
        for filename in tqdm(files_list):
            try:
                self.dataframes.append(pd.read_csv(filename))
            except Exception as e:
                logging.error(f"Error creating DataFrame for {filename}: {e}")
                raise ValueError(f"Error: {e}")

        if not self.dataframes:
            raise ValueError("No files found matching the specified extensions.")