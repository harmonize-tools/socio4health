import logging
import re
import threading

from scrapy.crawler import CrawlerProcess
from .standard_spider import StandardSpider
import zipfile
import shutil
import tempfile
import tarfile
import py7zr
import os
import requests
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_standard_spider(url, depth, down_ext, key_words):
    """Run the Scrapy spider to extract data from the given ``URL`` .

    Parameters
    ----------
    url : str
        The ``URL`` to start crawling from.
    depth : int
        The depth of the crawl.
    down_ext : list
        List of file extensions to download.
    key_words : list
        List of keywords to filter the crawled data.
    
    Returns
    -------
    ``None``
    """
    logging.getLogger('scrapy').propagate = False
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    logging.getLogger('twisted').setLevel(logging.CRITICAL)

    results = {}
    exception = None

    def _run_spider():
        nonlocal results, exception
        try:
            # Create fresh settings each time
            settings = {
                'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'ROBOTSTXT_OBEY': False,
                'LOG_LEVEL': 'ERROR',
                'DOWNLOAD_TIMEOUT': 30,
                'CONCURRENT_REQUESTS': 1,
                'TELNET_ENABLED': False,
                'EXTENSIONS': {
                    'scrapy.extensions.telnet.TelnetConsole': None,
                },
                'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
            }

            process = CrawlerProcess(settings)

            # Create spider instance to capture results
            spider = StandardSpider(
                url=url,
                depth=depth,
                ext=down_ext,
                key_words=key_words
            )

            process.crawl(
                StandardSpider,
                url=url,
                depth=depth,
                ext=down_ext,
                key_words=key_words
            )

            process.start(stop_after_crawl=True)

            # Store the results from the spider instance
            results = getattr(spider, 'links', {})

        except Exception as e:
            exception = e
            logging.error(f"Spider execution failed: {e}")

    # Run in a separate thread
    thread = threading.Thread(target=_run_spider)
    thread.daemon = True
    thread.start()
    thread.join(timeout=120)  # Timeout after 2 minutes

    if thread.is_alive():
        logging.warning("Spider thread timed out")
        return {}

    if exception:
        raise RuntimeError(f"Web scraping failed: {exception}")

    return results


def download_request(url, filename, download_dir):
    """Download a file from the specified ``URL`` and save it to the given directory.
    
    Parameters
    ----------
    url : str
        The ``URL`` of the file to download.
    filename : str
        The name to save the downloaded file.
    download_dir : str
        The directory where the file will be saved.
    
    Returns
    -------
    str
        The path to the downloaded file, or ``None`` if the download failed.

    """
    try:
        # Request to download
        response = requests.get(url, stream=True)
        response.raise_for_status()

        filepath = os.path.join(download_dir, filename)
        # Save file to the directory
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        logging.info(f"Successfully downloaded: {filename}")
        return filepath
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {filename} from {url}: {e}")
        return None


def compressed2files(input_archive, target_directory, down_ext, current_depth=0, max_depth=5, found_files=set()):
    """Extract files from a compressed archive and return the paths of the extracted files.

    Parameters
    ----------
    input_archive : str
        The path to the compressed archive file.
    target_directory : str
        The directory where the extracted files will be saved.
    down_ext : list
        A list of file extensions to filter the extracted files.
    current_depth : int, optional
        The current depth of extraction, used to limit recursion depth. Default is 0.
    max_depth : int, optional
        The maximum depth of extraction is to prevent infinite recursion. Default is 5.
    found_files : set, optional
        A set to keep track of already found files, used to avoid duplicates. Default is an empty set.

    Returns
    -------
    ``set``
        A ``set`` containing the paths of the extracted files that match the specified extensions.
    """

    logging.info(f"Processing (depth {current_depth}): {os.path.basename(input_archive)}")

    if current_depth > max_depth:
        logging.warning(f"Max depth {max_depth} reached")
        return set()

    found_files = set()

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Extract the archive
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
                logging.error(f"Unsupported format: {input_archive}")
                return set()

            # Process all extracted items
            for root, dirs, files in os.walk(temp_dir):
                for name in files:
                    item_path = os.path.join(root, name)
                    rel_path = os.path.relpath(item_path, temp_dir)

                    # Handle nested archives
                    if any(item_path.endswith(ext) for ext in ['.zip', '.7z', '.tar', '.gz']):
                        found_files.update(compressed2files(
                            item_path,
                            target_directory,
                            down_ext,
                            current_depth + 1,
                            max_depth
                        ))

                    # Handle regular files with matching extensions
                    else:
                        file_ext = os.path.splitext(name)[1].lower()
                        if any(file_ext == ext.lower() for ext in down_ext):
                            # Create unique filename with archive path hash
                            archive_hash = hashlib.md5(input_archive.encode()).hexdigest()[:8]
                            parent_folders = rel_path.replace(os.path.sep, '_')
                            unique_name = f"{archive_hash}_{parent_folders}"
                            dest_path = os.path.join(target_directory, unique_name)

                            # Ensure parent directory exists
                            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                            # Always move the file (no duplicate checking)
                            shutil.move(item_path, dest_path)
                            found_files.add(dest_path)
                            logging.info(f"Extracted: {unique_name}")

        except Exception as e:
            logging.error(f"Failed to process {input_archive}: {str(e)}", exc_info=True)
            return set()

    if not found_files:
        logging.warning(f"No matches in {os.path.basename(input_archive)}. Contents:")
        for root, dirs, files in os.walk(temp_dir):
            for f in files:
                logging.info(f"  Found: {os.path.relpath(os.path.join(root, f), temp_dir)}")

    if not found_files:
            logging.warning("No files found matching the specified extensions.")

    return found_files

def create_unique_path(archive_path, filename, target_dir):
    """Generate unique destination path"""
    archive_name = os.path.splitext(os.path.basename(archive_path))[0]
    base, ext = os.path.splitext(filename)
    unique_name = f"{archive_name}_{base}{ext}"
    return os.path.join(target_dir, unique_name)

def parse_fwf_dict(dict_df):
    """Parse a dictionary DataFrame to extract column names and fixed-width format specifications.

    Parameters
    ----------
    dict_df : pandas.DataFrame
        A DataFrame containing the dictionary information with columns:
        - 'variable_name': Column names
        - 'initial_position': Starting position (1-based) of each column
        - 'size': Width of each column

    Returns
    -------
    tuple
        A tuple containing:
        - A list of column names.
        - A list of tuples representing column specifications (start, end) where:
          - start is 0-based starting position
          - end is 0-based ending position (exclusive)

    Raises
    ------
    ValueError
        If no column names or sizes are found in the dictionary DataFrame.
    """

    # Extract column names
    colnames = dict_df['variable_name'].tolist()
    size = dict_df['size'].tolist()

    if not colnames:
        raise ValueError("No column names found in the dictionary DataFrame.")
    if not size:
        raise ValueError("No sizes found in the dictionary DataFrame.")

    colspecs = []
    for i,val in dict_df['initial_position'].items():
        start = int(val - 1)
        end = int(start + size[i])
        colspecs.append((start, end))

    return colnames, colspecs
