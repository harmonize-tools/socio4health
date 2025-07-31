"""
Extractor class for downloading and processing data files from various sources.
This class supports both online scraping and local file processing, handling compressed files, fixed-width files, and CSV formats.
It includes methods for downloading files, extracting data, and cleaning up after processing.
"""

import json
import shutil
from itertools import islice

from pathlib import Path
from typing import Optional, Union, Dict

import appdirs
import os
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
from tqdm import tqdm
import glob
from socio4health.utils.extractor_utils import run_standard_spider, compressed2files, download_request
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_default_data_dir():
    """
    Returns the default data directory for storing downloaded files.

    Returns
    -------
    ``Path``
        `pathlib.Path <https://docs.python.org/3/library/pathlib.html#pathlib.Path>`_ object representing the default data directory.
    Note
    ------
    This function ensures that the directory exists by creating it if necessary.
    Examples
    --------
    >>> default_dir = get_default_data_dir()
    >>> print(default_dir)
    /home/user/.local/share/socio4health
    >>> default_dir.exists()
    True
    """
    path = Path(appdirs.user_data_dir("socio4health"))
    logging.info(f"Default data directory: {path}")
    return path

class Extractor:
    """
    A class for extracting data from various sources, including online scraping and local file processing.
    This class supports downloading files, extracting data from compressed formats, and reading fixed-width or ``CSV`` files.
    It handles both online and local modes of operation, allowing for flexible data extraction workflows.

    Attributes
    ----------
    input_path : str
        The path to the input data source, which can be a URL or a local directory.
    depth : int
        The depth of web scraping to perform when input_path is a ``URL``.
    down_ext : list
        A list of file extensions to look for when downloading files. Available options include compressed formats such as ``.zip``, ``.7z``, ``.tar``, ``.gz``, and ``.tgz``, as well as other file types like ``.csv``, ``.txt``, etc. This list can be customized based on the expected file types.
    output_path : str
        The directory where downloaded files will be saved. Defaults to the user's data directory.
    key_words : list
        A list of keywords to filter downloadable files during web scraping.
    encoding : str
        The character encoding to use when reading files. Defaults to ``'latin1'``.
    is_fwf : bool
        Whether the files to be processed are fixed-width files (FWF). Defaults to ``False``.
    colnames : list
        Column names to use when reading fixed-width files. Required if is_fwf is ``True``.
    colspecs : list
        Column specifications for fixed-width files, defining the widths of each column. Required if ``is_fwf`` is ``True``.
    sep : str
        The separator to use when reading CSV files. Defaults to ','.
    ddtype : Union[str, Dict]
        The data type to use when reading files. Can be a single type or a dictionary mapping column names to types. Defaults to ``object``.
    dtype : Union[str, Dict]
        Data types to use when reading files with ``pandas``. Can be a string (e.g., ``'object'``) or a dictionary mapping column names to data types.
    engine : str
        The engine to use for reading Excel files (e.g., ``'openpyxl'`` or ``'xlrd'``). Leave as ``None`` to use the default engine based on file extension.
    sheet_name : Union[str, int, list, None]
        The name or index of the Excel sheet to read. Can also be a list to read multiple sheets or ``None`` to read all sheets. Defaults to the first sheet (``0``).
    geodriver : str
        The driver to use for reading geospatial files with ``geopandas.read_file()`` (e.g., ``'ESRI Shapefile'``, ``'KML'``, etc.). Optional.

    See Also
    -------
    Extractor.extract()
        Extracts data from the specified input path, either by scraping online or processing local files.

    Extractor.delete_download_folder(folder_path: Optional[str] = None) -> bool
        Safely deletes the download folder and all its contents, with safety checks to prevent accidental deletion of important directories.


    """
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
            ddtype: Union[str, Dict] = 'object',
            dtype: str = None,
            engine: str = None,
            sheet_name: str = None,
            geodriver: str = None
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
        self.READERS = {
            '.csv': self._read_csv,
            '.parquet': self._read_parquet,
            '.xls': self._read_excel,
            '.xlsx': self._read_excel,
            '.xlsm': self._read_excel,
            '.json': self._read_json,
            '.geojson': self._read_geospatial,
            '.shp': self._read_geospatial,
            '.kml': self._read_geospatial
        }
        os.makedirs(self.output_path, exist_ok=True)
        self.ddtype = ddtype
        self.dtype = dtype
        self.engine = engine
        self.sheet_name = sheet_name
        self.geodriver = geodriver
        if not input_path:
            raise ValueError("input_path must be provided")
        if is_fwf and (not colnames or not colspecs):
            raise ValueError("colnames and colspecs required for fixed-width files")

    def extract(self):
        """
        Extracts data from the specified input path, either by scraping online sources or processing local files.

        This method determines the operation mode based on the input path:\n
        - If the input path is a ``URL``, it performs online scraping to find downloadable files.\n
        - If the input path is a local directory, it processes files directly from that directory.\n

        Returns
        -------
        list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            List of `Dask <https://docs.dask.org>`_ DataFrames containing the extracted data.

        Raises
        ------
        ValueError
            If extraction fails due to an invalid input path, missing column specifications for fixed-width files,
            or if no valid data files are found after processing.
        """


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

    def _read_csv(self, filepath):
        # Read everything as text first to avoid dtype issues
        df = dd.read_csv(
            filepath,
            encoding=self.encoding,
            sep=self.sep if self.sep else ',',
            dtype=self.ddtype,
            assume_missing = True,
            on_bad_lines='warn'
        )
        if len(df.columns) == 1:
            # Try different separator if we only got one column
            df = dd.read_csv(
                filepath,
                encoding=self.encoding,
                sep=',' if self.sep != ',' else ';',
                dtype=self.ddtype,
                assume_missing=True,
                on_bad_lines='warn'
            )
        return df

    def _read_excel(self, filepath):
        return pd.read_excel(filepath,
                             sheet_name=self.sheet_name,
                             dtype=self.dtype,
                             engine=self.engine)
    
    def _read_parquet(self, filepath):
        return dd.read_parquet(filepath)

    def _read_json(self, filepath):
        with open(filepath, 'r', encoding=self.encoding) as f:
            return json.load(f)

    def _read_geospatial(self, filepath):
        return gpd.read_file(filepath)
    
    def _read_txt(self, filepath):
        return dd.read_csv(filepath, sep=self.sep or '\t', encoding=self.encoding, dtype=self.dtype)

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
                    dtype=self.ddtype,
                    assume_missing=True,
                    on_bad_lines='warn'
                )
            else:
                ext = Path(filepath).suffix.lower()
                if ext in self.READERS:
                    df = self.READERS[ext](filepath)
                else:
                    raise ValueError(f"Unsupported extension: {ext}")

            self.dataframes.append(df)

        except Exception as e:
            logging.error(f"Error reading {filepath}: {e}")
            raise ValueError(f"Error reading file: {e}")

    def delete_download_folder(self, folder_path: Optional[str] = None) -> bool:
        """
        Safely delete the download folder and all its contents.

        Args:
            `folder_path`: Optional path to delete (defaults to the `download_dir` used in extraction)

        Returns:
            bool: ``True`` if deletion was successful, ``False`` otherwise

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