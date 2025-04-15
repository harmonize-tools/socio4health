import json
import os
import shutil
from itertools import islice

from pathlib import Path
from typing import Optional, Union, Dict

import appdirs
import os
import dask.dataframe as dd
from tqdm import tqdm
import glob
from socio4health.utils.extractor_utils import run_standard_spider, compressed2files, download_request
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_default_data_dir():
    """Return platform-appropriate default data directory"""
    path = Path(appdirs.user_data_dir("socio4health"))
    logging.info(f"Default data directory: {path}")
    return path

class Extractor:
    def __init__(
            self,
            path: str = None,
            url: str = None,
            depth: int = None,
            down_ext: list = None,
            download_dir: str = None,
            key_words: list = None,
            encoding: str = 'latin1',
            is_fwf: bool = False,
            colnames: list = None,
            colspecs: list = None,
            sep: str = None,
            dtype: Union[str, Dict] = 'object'
    ):
        self.compressed_ext = ['.zip', '.7z', '.tar', '.gz', '.tgz']
        self.url = url
        self.depth = depth
        self.down_ext = down_ext if down_ext is not None else []
        self.key_words = key_words if key_words is not None else []
        self.path = path
        self.mode = -1
        self.dataframes = []
        self.encoding = encoding
        self.is_fwf = is_fwf
        self.colnames = colnames
        self.colspecs = colspecs
        self.sep = sep
        self.download_dir = download_dir or str(get_default_data_dir())
        os.makedirs(self.download_dir, exist_ok=True)
        self.dtype = dtype

        if path and url:
            raise ValueError(
                "Both 'path' and 'url' cannot be specified. "
                "Choose either local mode (path) or online mode (url)."
            )
        elif not path and not url:
            raise ValueError(
                "Either 'path' (for local files) or 'url' (for web scraping) must be provided."
            )

        if url:
            self.mode = 0
            if depth is None:
                raise ValueError("'depth' must be specified in online mode.")
            if self.download_dir is None:
                raise ValueError("'download_dir' must be specified in online mode.")
            if not self.down_ext:
                raise ValueError("'down_ext' (download extensions) must be specified in online mode.")
        elif path:
            self.mode = 1
            if not self.down_ext:
                raise ValueError("'down_ext' (file extensions) must be specified in local mode.")

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

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            logging.info(f"Created download directory: {self.download_dir}")

        downloaded_files = []

        if links:
            for filename, url in tqdm(links.items()):
                filepath = download_request(url, filename, self.download_dir)
                downloaded_files.append(filepath)
                extracted_extensions.add(filepath.split(".")[-1])
        else:
            try:
                filename = self.url.split("/")[-1]
                if len(filename.split(".")) == 1:
                    filename += ".zip"
                filepath = download_request(self.url, filename, self.download_dir)
                downloaded_files.append(filepath)
                logging.info(f"Successfully downloaded {filename} file.")
            except Exception as e:
                logging.error(f"Error downloading: {e}")
                raise ValueError(
                    f"No files were found at the specified link. Please verify the URL, search depth, and file extensions.")

        # Process downloaded files using local mode logic
        files_list = []
        compressed_list = []
        extracted_files = []

        for filepath in downloaded_files:
            if any(filepath.endswith(ext) for ext in self.compressed_ext):
                compressed_list.append(filepath)
                extracted_files.extend(
                    compressed2files(input_archive=filepath, target_directory=self.download_dir,
                                     down_ext=self.down_ext))
            else:
                files_list.append(filepath)

        for filename in tqdm(files_list):
            self._read_file(filename)

        for extracted_file in tqdm(extracted_files):
            self._read_file(extracted_file)

        # Clean up
        os.remove("Output_scrap.json")

        if not self.dataframes:
            logging.warning(
                f"\nSuccessfully downloaded files with the following extensions: {extracted_extensions}. "
                "However, it appears there are no files matching your requested extensions: {self.down_ext} within any compressed files. "
                "Please ensure the requested file extensions are correct and present within the compressed files.")
            raise ValueError("No valid data files found after extraction.")

    def _extract_local_mode(self):
        logging.info("Extracting data in local mode...")
        files_list = []
        compressed_list = []

        compressed_inter = set(self.compressed_ext) & set(self.down_ext)
        iter_ext = list(compressed_inter) + list(set(self.down_ext) - compressed_inter)

        extracted_files = []

        for ext in iter_ext:
            full_pattern = os.path.join(self.path, f"*{ext}")
            if ext in self.compressed_ext:
                compressed_list.extend(glob.glob(full_pattern))
                for filepath in compressed_list:
                    # Use same directory as source if download_dir not specified
                    target_dir = self.download_dir if self.download_dir else os.path.dirname(filepath)
                    extracted_files.extend(
                        compressed2files(
                            input_archive=filepath,
                            target_directory=target_dir,
                            down_ext=self.down_ext
                        )
                    )
            else:
                files_list.extend(glob.glob(full_pattern))

        for filename in tqdm(files_list):
            self._read_file(filename)

        for extracted_file in tqdm(extracted_files):
            self._read_file(extracted_file)

        if not self.dataframes:
            logging.warning("No files found matching the specified extensions.")

    def _read_file(self, filepath):
        try:
            if self.is_fwf:
                if not self.colnames or not self.colspecs:
                    logging.error("Column specs required for fixed-width files")
                    raise ValueError("Column specs required for fixed-width files")
                df = dd.read_fwf(
                    filepath,
                    colspecs=self.colspecs,
                    names=self.colnames,
                    encoding=self.encoding,
                    dtype=self.dtype,
                    assume_missing=True,
                    on_bad_lines='warn'
                )
            else:
                # Read everything as text first to avoid dtype issues
                df = dd.read_csv(
                    filepath,
                    encoding=self.encoding,
                    sep=self.sep if self.sep else ',',
                    dtype=self.dtype,
                    assume_missing = True,
                    on_bad_lines='warn'
                )
                if len(df.columns) == 1:
                    # Try different separator if we only got one column
                    df = dd.read_csv(
                        filepath,
                        encoding=self.encoding,
                        sep=',' if self.sep != ',' else ';',
                        dtype=self.dtype,
                        assume_missing=True,
                        on_bad_lines='warn'
                    )

            self.dataframes.append(df)

        except Exception as e:
            logging.error(f"Error reading {filepath}: {e}")
            raise ValueError(f"Error reading file: {e}")

    def delete_download_folder(self, folder_path: Optional[str] = None) -> bool:
        """
        Safely delete the download folder and all its contents.

        Args:
            folder_path: Optional path to delete (defaults to the download_dir used in extraction)

        Returns:
            bool: True if deletion was successful, False otherwise

        Raises:
            ValueError: If no folder path is provided and no download_dir exists
            OSError: If folder deletion fails
        """
        # Determine which folder to delete
        target_path = Path(folder_path) if folder_path else Path(self.download_dir)

        # Safety checks
        if not target_path.exists():
            logging.warning(f"Folder {target_path} does not exist - nothing to delete")
            return False

        if not target_path.is_dir():
            raise ValueError(f"Path {target_path} is not a directory")

        # Prevent accidental deletion of important directories
        protected_paths = [
            Path.home(),
            Path("/"),
            Path.cwd(),
            Path(appdirs.user_data_dir())  # If using appdirs
        ]

        if any(target_path == p or target_path in p.parents for p in protected_paths):
            raise ValueError(f"Cannot delete protected directory: {target_path}")

        try:
            logging.info(f"Deleting folder: {target_path}")
            shutil.rmtree(target_path)
            logging.info("Folder deleted successfully")
            return True

        except Exception as e:
            logging.error(f"Failed to delete folder {target_path}: {str(e)}")
            raise OSError(f"Folder deletion failed: {str(e)}")