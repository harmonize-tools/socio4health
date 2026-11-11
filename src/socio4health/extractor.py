"""
Extractor class for downloading and processing data files from various sources.
This class supports both online scraping and local file processing, handling compressed files, fixed-width files, and ``CSV`` formats.
It includes methods for downloading files, extracting data, and cleaning up after processing.
"""

import hashlib
import json
import re
import shutil
import time
from itertools import islice

from pathlib import Path
from typing import Optional, Union, Dict

import appdirs
import os
import pandas as pd
from socio4health.utils.deps import import_optional
import pyreadstat
import dask.dataframe as dd
import glob
from socio4health.utils.extractor_utils import compressed2files, download_request
from importlib import import_module
from socio4health.utils.log import logger, progress, progress_bar



def s4h_get_default_data_dir():
    """
    Returns the default data directory for storing downloaded files.

    Returns
    -------
    ``Path``
        `pathlib.Path <https://docs.python.org/3/library/pathlib.html#pathlib.Path>`_ object representing the default data directory.
    Note
    ------
    This function ensures that the directory exists by creating it if necessary.

    """
    path = Path(appdirs.user_data_dir("socio4health"))
    logger.debug(f"Default data directory: {path}")
    return path

class Extractor:
    """
    A class for extracting data from various sources, including online scraping and local file processing.
    This class supports downloading files, extracting data from compressed formats, and reading fixed-width or ``CSV`` files.
    It handles both online and local modes of operation, allowing for flexible data extraction workflows.

    Attributes
    ----------
    input_path : str
        The path to the input data source, which can be a ``URL`` or a local directory.
    depth : int
        The depth of web scraping to perform when input_path is a ``URL``.
    down_ext : list
        A list of file extensions to look for when downloading files. Extensions are case-insensitive:
        ``'.csv'`` also matches ``DATA.CSV``, so there is no need to list both. Available options include compressed formats such as ``.zip``, ``.7z``, ``.tar``, ``.gz``, and ``.tgz``, as well as other file types like ``.csv``, ``.txt``, etc. This list can be customized based on the expected file types.
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
        The separator to use when reading ``CSV`` files. Defaults to ``,``.
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
    delete_zip_after : bool
        If True, delete zip/compressed files after extraction. Defaults to False.
    on_bad_lines : str
        How to handle bad lines when reading ``CSV`` files. Options are 'error', 'warn', or 'skip'. Defaults to 'warn'.
    file_pattern : str
        Regular expression matched (``re.search``) against the name of each downloaded or extracted file.
        Only matching files are read. Useful when an archive contains several file types and only some
        of them follow the given ``colspecs`` (e.g. ``r"Domicilios"``). Defaults to ``None`` (read every file).
    cache_parquet : bool
        If ``True``, fixed-width, ``CSV`` and ``TXT`` files are converted once to Parquet (stored next to the
        source file as ``<file>.parquet``) and read from there. Parsing text is the slowest step for large
        datasets; with Parquet, later computations only read the columns they use. The first extraction
        takes longer because it parses every file once. The cache is rebuilt automatically if the source
        file or the reading options (``colnames``, ``colspecs``, ``encoding``...) change. Defaults to ``False``.

    Important
    ------
    In case ``is_fwf`` is ``True`` and fixed-width files are given, both ``colnames`` and ``colspecs`` must be provided.

    See Also
    -------
    Extractor.s4h_extract()
        Extracts data from the specified input path, either by scraping online or processing local files.

    Extractor.s4h_delete_download_folder(folder_path: Optional[str] = None) -> bool
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
            geodriver: str = None,
            delete_zip_after: bool = False,
            on_bad_lines: str = 'warn',
            file_pattern: str = None,
            cache_parquet: bool = False
        ):
        self.compressed_ext = ['.zip', '.7z', '.tar', '.gz', '.tgz']
        self.depth = depth
        self.down_ext = self._normalize_extensions(down_ext)
        self.key_words = key_words if key_words is not None else []
        self.input_path = input_path
        self.mode = -1
        self.dataframes = []
        self.encoding = encoding
        self.is_fwf = is_fwf
        self.colnames = colnames
        self.colspecs = colspecs
        self.sep = sep
        self.output_path = output_path or str(s4h_get_default_data_dir())
        self.READERS = {
            '.csv': self._read_csv,
            '.txt': self._read_txt,
            '.parquet': self._read_parquet,
            '.xls': self._read_excel,
            '.xlsx': self._read_excel,
            '.xlsm': self._read_excel,
            '.json': self._read_json,
            '.geojson': self._read_geospatial,
            '.shp': self._read_geospatial,
            '.kml': self._read_geospatial,
            '.sav': self._read_sav
        }
        os.makedirs(self.output_path, exist_ok=True)
        self.ddtype = ddtype
        self.dtype = dtype
        self.engine = engine
        self.sheet_name = sheet_name
        self.geodriver = geodriver
        self.delete_zip_after = delete_zip_after
        self.on_bad_lines = on_bad_lines
        self.file_pattern = re.compile(file_pattern) if file_pattern else None
        self.cache_parquet = cache_parquet
        if not input_path:
            raise ValueError("input_path must be provided")
        if is_fwf and (not colnames or not colspecs):
            raise ValueError("colnames and colspecs required for fixed-width files")

    @staticmethod
    def _normalize_extensions(extensions):
        """Lowercase, add the leading dot and drop duplicates: ``['.CSV', 'csv', '.zip']`` -> ``['.csv', '.zip']``.
        Extensions are always compared case-insensitively, so ``'.csv'`` also matches ``FILE.CSV``."""
        normalized = []
        for ext in extensions or []:
            ext = str(ext).strip().lower()
            if ext and not ext.startswith('.'):
                ext = '.' + ext
            if ext and ext not in normalized:
                normalized.append(ext)
        return normalized

    @staticmethod
    def _has_extension(path, extensions):
        return str(path).lower().endswith(tuple(extensions))

    @staticmethod
    def _is_generated_file(path):
        """Files socio4health writes itself (Parquet cache, its signature, unfinished downloads/extractions)."""
        lower = path.lower()
        parent = os.path.basename(os.path.dirname(lower))
        return lower.endswith(('.parquet.json', '.part')) or parent.endswith('.parquet')

    def s4h_extract(self):
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


        logger.info("Starting data extraction...")
        try:
            if self.input_path and self.input_path.startswith("http"):
                self._extract_online_mode()
            elif self.input_path and os.path.isdir(self.input_path):
                self._extract_local_mode()
            logger.info("Extraction completed successfully.")
        except Exception as e:
            logger.error(f"Exception while extracting data: {e}")
            raise ValueError(f"Extraction failed: {str(e)}")

        if not self.dataframes:
            logger.warning("No data was extracted. The extraction process returned an empty result.")

        return self.dataframes

    def _extract_online_mode(self):
        """Optimized online data extraction with better error handling and progress tracking"""
        logger.info("Extracting data in online mode...")

        # For direct file downloads (like your zip case), skip scraping
        if self._has_extension(self.input_path, self.down_ext + self.compressed_ext):
            logger.info("Detected direct file download URL - skipping scraping")
            try:
                filename = self.input_path.split("/")[-1]
                if not self._has_extension(filename, self.down_ext + self.compressed_ext):
                    filename += ".zip"  # Add extension if missing

                # Create download directory if needed
                os.makedirs(self.output_path, exist_ok=True)
                filepath = os.path.join(self.output_path, filename)

                with progress_bar(1, desc="Downloading") as bar:
                    filepath = download_request(
                        self.input_path,
                        filename,
                        self.output_path,
                        on_progress=self._download_progress(bar, filename)
                    )
                    bar.update(1)
                if not filepath:
                    raise ValueError(f"could not download {filename} (see error above)")

                # Process the downloaded file(s) using local mode logic
                files_to_process = []
                if self._has_extension(filepath, self.compressed_ext):
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
                logger.error(f"Direct download failed: {e}")
                raise ValueError(f"Failed to download {self.input_path}: {str(e)}")

        # Step 1: Scrape for downloadable files
        try:
            logger.info(f"Scraping URL: {self.input_path} with depth {self.depth}")
            # Import and run the spider only when needed (lazy)
            extractor_utils = import_module('socio4health.utils.extractor_utils')
            run_standard_spider = getattr(extractor_utils, 'run_standard_spider')
            run_standard_spider(self.input_path, self.depth, self.down_ext, self.key_words)

            # Read scraped links
            with open("Output_scrap.json", 'r', encoding='utf-8') as file:
                links = json.load(file)
        except Exception as e:
            logger.error(f"Failed during web scraping: {e}")
            raise ValueError(f"Web scraping failed: {str(e)}")

        # Step 2: Filter and confirm files to download
        if not links:
            logger.warning("No downloadable files found matching criteria. Returning empty extraction.")
            return
        logger.info(f"Found {len(links)} files to download")

        # Handle large number of files with user confirmation
        if len(links) > 30:
            user_input = input(
                f"Found {len(links)} files. Download all? [Y/N] (N will prompt for count): ").strip().lower()
            if user_input != 'y':
                try:
                    files2download = int(input("Enter number of files to download: "))
                    links = dict(islice(links.items(), max(1, files2download)))
                except ValueError:
                    logger.warning("Invalid input, proceeding with first 30 files")
                    links = dict(islice(links.items(), 30))

        # Step 3: Download files with progress tracking
        downloaded_files = []
        failed_downloads = []

        os.makedirs(self.output_path, exist_ok=True)
        logger.info(f"Downloading {len(links)} files to '{self.output_path}'")

        with progress_bar(len(links), desc="Downloading") as bar:
            for filename, url in links.items():
                bar.set_postfix_str(filename)
                try:
                    filepath = download_request(url, filename, self.output_path,
                                                on_progress=self._download_progress(bar, filename))
                    if filepath:
                        downloaded_files.append(filepath)
                    else:
                        failed_downloads.append((filename, "see error above"))
                except Exception as e:
                    logger.warning(f"Failed to download {filename}: {e}")
                    failed_downloads.append((filename, str(e)))
                bar.update(1)
            bar.set_postfix_str("")

        if not downloaded_files:
            logger.warning("No files were successfully downloaded. Returning empty extraction.")
            return

        logger.info(f"Downloaded {len(downloaded_files)}/{len(links)} files")
        if failed_downloads:
            logger.warning(f"Failed to download {len(failed_downloads)} files: "
                           f"{', '.join(name for name, _ in failed_downloads)}")

        # Step 4: Process downloaded files using the local mode logic
        self._process_downloaded_files(downloaded_files)

        # Cleanup
        try:
            os.remove("Output_scrap.json")
        except Exception as e:
            logger.warning(f"Could not remove scrap file: {e}")

        if not self.dataframes:
            logger.warning("No valid data files found after processing. Returning empty extraction.")
            return

    @staticmethod
    def _download_progress(bar, filename, min_interval=0.5):
        """Callback for download_request: shows the MB downloaded of the current file next to the bar."""
        last = [0.0]

        def on_progress(done, total):
            now = time.monotonic()
            if now - last[0] < min_interval and done != total:
                return
            last[0] = now
            size = f"{done / 1e6:.0f}/{total / 1e6:.0f} MB" if total else f"{done / 1e6:.0f} MB"
            bar.set_postfix_str(f"{filename} {size}")

        return on_progress

    def _process_downloaded_files(self, downloaded_files):
        """Process downloaded files using local mode logic"""
        downloaded_files = [f for f in downloaded_files if f]
        archives = [f for f in downloaded_files if self._has_extension(f, self.compressed_ext)]
        files_to_process = [f for f in downloaded_files if f not in archives]
        files_to_process.extend(self._extract_archives(archives, self.output_path))

        # Process all files (both direct downloads and extracted files)
        self._process_files_locally(files_to_process)

        if self.delete_zip_after:
            self._delete_archives(archives)

    def _extract_archives(self, archives, parent_dir):
        """Extract each archive into ``<parent_dir>/<archive name>/`` and return the extracted files."""
        extracted_files = []
        if not archives:
            return extracted_files
        for filepath in progress(archives, desc="Extracting", total=len(archives), unit="archive",
                                 label=os.path.basename):
            base_name = os.path.splitext(os.path.basename(filepath))[0]
            target_dir = os.path.join(parent_dir or os.path.dirname(filepath), base_name)
            os.makedirs(target_dir, exist_ok=True)
            extracted_files.extend(
                compressed2files(
                    input_archive=filepath,
                    target_directory=target_dir,
                    down_ext=self.down_ext
                )
            )
        logger.info(f"Extracted {len(extracted_files)} files from {len(archives)} archives")
        return extracted_files

    @staticmethod
    def _delete_archives(archives):
        deleted = 0
        for zip_path in archives:
            try:
                os.remove(zip_path)
                deleted += 1
                logger.debug(f"Deleted archive after extraction: {zip_path}")
            except Exception as e:
                logger.warning(f"Could not delete archive {zip_path}: {e}")
        if deleted:
            logger.info(f"Deleted {deleted} archives after extraction")

    def _process_files_locally(self, files):
        """Shared local processing logic used by both modes"""
        valid_files = 0

        if self.file_pattern:
            skipped = [f for f in files if not self.file_pattern.search(os.path.basename(f))]
            files = [f for f in files if self.file_pattern.search(os.path.basename(f))]
            if skipped:
                logger.info(f"Skipping {len(skipped)} files not matching file_pattern '{self.file_pattern.pattern}'")

        self._caches_built = 0
        for filepath in progress(files, desc="Reading", total=len(files), label=os.path.basename):
            try:
                if os.path.getsize(filepath) == 0:
                    logger.warning(f"Skipping empty file: {filepath}")
                    continue

                self._read_file(filepath)
                valid_files += 1
            except Exception as e:
                logger.warning(f"Error processing {filepath}: {e}")

        summary = f"Read {valid_files}/{len(files)} files"
        if self.cache_parquet:
            reused = valid_files - self._caches_built
            summary += f" (Parquet cache: {self._caches_built} built, {reused} reused)"
        logger.info(summary)

    def _extract_local_mode(self):
        """Local mode extraction that now uses the shared processing logic"""
        logger.info("Extracting data in local mode...")
        files_list = []
        compressed_list = []

        # Match extensions case-insensitively on every OS (glob is case-sensitive on Linux/macOS)
        wanted_compressed = [ext for ext in self.down_ext if ext in self.compressed_ext]
        wanted_files = [ext for ext in self.down_ext if ext not in self.compressed_ext]
        for path in sorted(glob.glob(os.path.join(self.input_path, "**", "*"), recursive=True)):
            if not os.path.isfile(path) or self._is_generated_file(path):
                continue
            if self._has_extension(path, wanted_compressed):
                compressed_list.append(path)
            elif self._has_extension(path, wanted_files):
                files_list.append(path)

        extracted_files = self._extract_archives(compressed_list, self.output_path)
        # Process all files using the shared method
        self._process_files_locally(files_list + extracted_files)

        if self.delete_zip_after:
            self._delete_archives(compressed_list)

        if not self.dataframes:
            logger.warning("No files found matching the specified extensions.")

    def _read_csv(self, filepath):
        # Read everything as text first to avoid dtype issues
        df = dd.read_csv(
            filepath,
            encoding=self.encoding,
            sep=self.sep if self.sep else ',',
            dtype=self.ddtype,
            assume_missing = True,
            on_bad_lines=self.on_bad_lines
        )
        if len(df.columns) == 1:
            # Try different separator if we only got one column
            df = dd.read_csv(
                filepath,
                encoding=self.encoding,
                sep=',' if self.sep != ',' else ';',
                dtype=self.ddtype,
                assume_missing=True,
                on_bad_lines=self.on_bad_lines
            )
        return df

    def _read_excel(self, filepath):
        return dd.from_pandas(pd.read_excel(filepath,
                             sheet_name=self.sheet_name,
                             dtype=self.dtype,
                             engine=self.engine))
    
    def _read_parquet(self, filepath):
        return dd.read_parquet(filepath)

    def _read_json(self, filepath):
        with open(filepath, 'r', encoding=self.encoding) as f:
            return dd.from_pandas(json.load(f))

    def _read_geospatial(self, filepath):
        gpd = import_optional('geopandas', extra='geo')
        return gpd.read_file(filepath)
    
    def _read_txt(self, filepath):
        return dd.read_csv(filepath, sep=self.sep or '\t', encoding=self.encoding, dtype=self.dtype or 'object')

    def _read_sav(self, filepath):
        df, meta = pyreadstat.read_sav(filepath, encoding=self.encoding)
        return df
    
    def _read_fwf(self, filepath):
        if not self.colnames or not self.colspecs:
            logger.error("Column specs required for fixed-width files")
            raise ValueError("Column specs required for fixed-width files")
        return dd.read_fwf(
            filepath,
            colspecs=self.colspecs,
            names=self.colnames,
            encoding=self.encoding,
            dtype=self.ddtype,
            assume_missing=True,
            on_bad_lines='warn'
        )

    @staticmethod
    def _quick_fingerprint(filepath, chunk_size=1 << 20):
        """Hash of the first and last MiB of a file. Unlike the modification time, it does not change
        when the same archive is extracted again, and it is cheap even for multi-GB files."""
        digest = hashlib.sha1()
        size = os.path.getsize(filepath)
        with open(filepath, 'rb') as f:
            digest.update(f.read(chunk_size))
            if size > chunk_size:
                f.seek(max(chunk_size, size - chunk_size))
                digest.update(f.read(chunk_size))
        return digest.hexdigest()

    def _cache_signature(self, filepath, reader_name):
        """Everything that changes the parsed result; if any of it changes, the Parquet cache is rebuilt."""
        return {
            'reader': reader_name,
            'source_size': os.path.getsize(filepath),
            'source_fingerprint': self._quick_fingerprint(filepath),
            'colnames': self.colnames if self.is_fwf else None,
            'colspecs': [list(c) for c in self.colspecs] if self.is_fwf else None,
            'encoding': self.encoding,
            'sep': self.sep,
            'ddtype': self.ddtype,
            'dtype': self.dtype,
        }

    def _read_with_parquet_cache(self, filepath, reader, reader_name):
        """Parse a text file once, store it as Parquet next to the source and read it from there."""
        cache_path = f"{filepath}.parquet"
        signature_path = f"{cache_path}.json"
        signature = json.loads(json.dumps(self._cache_signature(filepath, reader_name), default=str))

        try:
            with open(signature_path, 'r', encoding='utf-8') as f:
                if json.load(f) == signature and os.path.isdir(cache_path):
                    return dd.read_parquet(cache_path)
        except (OSError, ValueError):
            pass

        logger.debug(f"Building Parquet cache for {os.path.basename(filepath)} (only done once)")
        self._caches_built = getattr(self, '_caches_built', 0) + 1
        if os.path.exists(signature_path):
            os.remove(signature_path)
        reader(filepath).to_parquet(cache_path, write_index=False, overwrite=True)
        # The signature is written last, so an interrupted conversion is never reused
        with open(signature_path, 'w', encoding='utf-8') as f:
            json.dump(signature, f)
        return dd.read_parquet(cache_path)

    def _read_file(self, filepath):
        try:
            df = None
            ext = Path(filepath).suffix.lower()
            if self.is_fwf:
                reader, reader_name = self._read_fwf, 'fwf'
            elif ext in self.READERS:
                reader, reader_name = self.READERS[ext], ext
            else:
                logger.warning(f"Unsupported extension: {ext}")
                return

            if self.cache_parquet and reader_name in ('fwf', '.csv', '.txt'):
                df = self._read_with_parquet_cache(filepath, reader, reader_name)
            else:
                df = reader(filepath)

            # Do not call len() on Dask DataFrames: it reads the whole file just to count rows.
            # Empty files are already skipped by size in _process_files_locally.
            if df is not None and (isinstance(df, dd.DataFrame) or len(df) != 0):
                df['filename'] = os.path.basename(filepath)
                self.dataframes.append(df)

        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
            raise ValueError(f"Error reading file: {e}")

    def s4h_delete_download_folder(self, folder_path: Optional[str] = None) -> bool:
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
            logger.warning(f"Folder {target_path} does not exist - nothing to delete")
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
            logger.info(f"Deleting folder: {target_path}")
            shutil.rmtree(target_path)
            logger.info("Folder deleted successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to delete folder {target_path}: {str(e)}")
            raise OSError(f"Folder deletion failed: {str(e)}")