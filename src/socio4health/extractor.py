import json
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
            input_path: str = None,
            depth: int = None,
            down_ext: list = None,
            output_path: str = None,
            key_words: list = None,
            encoding: str = 'latin1',
            is_fwf: bool = False,
            colnames: list = None,
            colspecs: list = None,
            sep: str = None,
            dtype: Union[str, Dict] = 'object'
    ):
        self.compressed_ext = ['.zip', '.7z', '.tar', '.gz', '.tgz']
        self.depth = depth
        self.down_ext = down_ext if down_ext is not None else []
        self.key_words = key_words if key_words is not None else []
        self.input_path = input_path
        self.mode = -1
        self.dataframes = []
        self.encoding = encoding
        self.is_fwf = is_fwf
        self.colnames = colnames
        self.colspecs = colspecs
        self.sep = sep
        self.output_path = output_path or str(get_default_data_dir())
        os.makedirs(self.output_path, exist_ok=True)
        self.dtype = dtype
        if not input_path:
            raise ValueError("input_path must be provided")
        if is_fwf and (not colnames or not colspecs):
            raise ValueError("colnames and colspecs required for fixed-width files")

    def extract(self):
        logging.info("----------------------")
        logging.info("Starting data extraction...")
        try:
            if self.input_path and self.input_path.startswith("http"):
                self._extract_online_mode()
            elif self.input_path and os.path.isdir(self.input_path):
                self._extract_local_mode()
            logging.info("Extraction completed successfully.")
        except Exception as e:
            logging.error(f"Exception while extracting data: {e}")
            raise ValueError(f"Extraction failed: {str(e)}")

        return self.dataframes

    def _extract_online_mode(self):
        """Optimized online data extraction with better error handling and progress tracking"""
        logging.info("Extracting data in online mode...")

        # For direct file downloads (like your zip case), skip scraping
        if self.input_path.lower().endswith(tuple(self.down_ext + self.compressed_ext)):
            logging.info("Detected direct file download URL - skipping scraping")
            try:
                filename = self.input_path.split("/")[-1]
                if not any(filename.endswith(ext) for ext in self.down_ext + self.compressed_ext):
                    filename += ".zip"  # Add extension if missing

                # Create download directory if needed
                os.makedirs(self.output_path, exist_ok=True)
                filepath = os.path.join(self.output_path, filename)

                # Download with progress bar for large files
                logging.info(f"Downloading large file ({filename})...")
                with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1) as pbar:
                    filepath = download_request(
                        self.input_path,
                        filename,
                        self.output_path
                    )

                # Process the downloaded file(s) using local mode logic
                files_to_process = []
                if any(filepath.endswith(ext) for ext in self.compressed_ext):
                    extracted_files = compressed2files(
                        input_archive=filepath,
                        target_directory=self.output_path,
                        down_ext=self.down_ext
                    )
                    files_to_process.extend(extracted_files)
                else:
                    files_to_process.append(filepath)

                self._process_files_locally(files_to_process)
                return

            except Exception as e:
                logging.error(f"Direct download failed: {e}")
                raise ValueError(f"Failed to download {self.input_path}: {str(e)}")

        # Step 1: Scrape for downloadable files
        try:
            logging.info(f"Scraping URL: {self.input_path} with depth {self.depth}")
            run_standard_spider(self.input_path, self.depth, self.down_ext, self.key_words)

            # Read scraped links
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                links = json.load(file)
        except Exception as e:
            logging.error(f"Failed during web scraping: {e}")
            raise ValueError(f"Web scraping failed: {str(e)}")

        # Step 2: Filter and confirm files to download
        if not links:
            logging.error("No downloadable files found matching criteria")
            raise ValueError("No files found matching the specified extensions and keywords")

        # Handle large number of files with user confirmation
        if len(links) > 30:
            user_input = input(
                f"Found {len(links)} files. Download all? [Y/N] (N will prompt for count): ").strip().lower()
            if user_input != 'y':
                try:
                    files2download = int(input("Enter number of files to download: "))
                    links = dict(islice(links.items(), max(1, files2download)))
                except ValueError:
                    logging.warning("Invalid input, proceeding with first 30 files")
                    links = dict(islice(links.items(), 30))

        # Step 3: Download files with progress tracking
        downloaded_files = []
        failed_downloads = []

        os.makedirs(self.output_path, exist_ok=True)
        logging.info(f"Downloading files to: {self.output_path}")

        for filename, url in tqdm(links.items(), desc="Downloading files"):
            try:
                filepath = download_request(url, filename, self.output_path)
                downloaded_files.append(filepath)
            except Exception as e:
                logging.warning(f"Failed to download {filename}: {e}")
                failed_downloads.append((filename, str(e)))

        if not downloaded_files:
            logging.error("No files were successfully downloaded")
            raise ValueError("All download attempts failed")

        if failed_downloads:
            logging.warning(f"Failed to download {len(failed_downloads)} files")

        # Step 4: Process downloaded files using the local mode logic
        self._process_downloaded_files(downloaded_files)

        # Cleanup
        try:
            os.remove("Output_scrap.json")
        except Exception as e:
            logging.warning(f"Could not remove scrap file: {e}")

        if not self.dataframes:
            logging.error("No valid data files found after processing")
            raise ValueError("No data could be extracted from downloaded files")

    def _process_downloaded_files(self, downloaded_files):
        """Process downloaded files using local mode logic"""
        files_to_process = []

        # Classify and extract compressed files
        for filepath in downloaded_files:
            if any(filepath.endswith(ext) for ext in self.compressed_ext):
                extracted = compressed2files(
                    input_archive=filepath,
                    target_directory=self.output_path,
                    down_ext=self.down_ext
                )
                files_to_process.extend(extracted)
            else:
                files_to_process.append(filepath)

        # Process all files (both direct downloads and extracted files)
        self._process_files_locally(files_to_process)

    def _process_files_locally(self, files):
        """Shared local processing logic used by both modes"""
        valid_files = 0

        for filepath in tqdm(files, desc="Processing files"):
            try:
                if os.path.getsize(filepath) == 0:
                    logging.warning(f"Skipping empty file: {filepath}")
                    continue

                self._read_file(filepath)
                valid_files += 1
            except Exception as e:
                logging.warning(f"Error processing {filepath}: {e}")

        logging.info(f"Successfully processed {valid_files}/{len(files)} files")

    def _extract_local_mode(self):
        """Local mode extraction that now uses the shared processing logic"""
        logging.info("Extracting data in local mode...")
        files_list = []
        compressed_list = []

        compressed_inter = set(self.compressed_ext) & set(self.down_ext)
        iter_ext = list(compressed_inter) + list(set(self.down_ext) - compressed_inter)

        extracted_files = []

        for ext in iter_ext:
            full_pattern = os.path.join(self.input_path, f"*{ext}")
            if ext in self.compressed_ext:
                compressed_list.extend(glob.glob(full_pattern))
                for filepath in compressed_list:
                    # Use same directory as source if download_dir not specified
                    target_dir = self.output_path if self.output_path else os.path.dirname(filepath)
                    extracted_files.extend(
                        compressed2files(
                            input_archive=filepath,
                            target_directory=target_dir,
                            down_ext=self.down_ext
                        )
                    )
            else:
                files_list.extend(glob.glob(full_pattern))

        # Process all files using the shared method
        self._process_files_locally(files_list + extracted_files)

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
        target_path = Path(folder_path) if folder_path else Path(self.output_path)

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