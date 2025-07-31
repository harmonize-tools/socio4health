import logging
from datetime import datetime
import re

from scrapy.crawler import CrawlerProcess
from .standard_spider import StandardSpider
import zipfile
import shutil
import tempfile
import tarfile
import py7zr
import os
import requests

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

    process = CrawlerProcess({
        'LOG_LEVEL': 'CRITICAL',
        'LOG_ENABLED': False,
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
        # Other Scrapy settings
    })
    process.crawl(StandardSpider, url=url, depth=depth, down_ext=down_ext, key_words=key_words)
    process.start()


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


    if current_depth > max_depth:
        logging.warning(f"Reached max depth of {max_depth}. Stopping further extraction.")
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
            logging.error(f"Unsupported archive format: {input_archive}")
            return None

        # Ensure the target directory exists
        if not os.path.exists(target_directory):
            os.makedirs(target_directory)
            logging.info(f"Created target directory: {target_directory}")

        # Process the extracted contents
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # Check if file is a nested archive and process it
                if any(file_path.endswith(ext) for ext in ['.zip', '.7z', '.tar', '.gz', '.tgz']):
                    if current_depth < max_depth:
                        found_files |= set(
                            compressed2files(file_path, target_directory, down_ext, current_depth + 1, max_depth,
                                             found_files))
                elif f".{file.split('.')[-1].lower()}" in down_ext:
                    # Generate a unique filename
                    base_name, ext = os.path.splitext(file)
                    parent = os.path.splitext(os.path.basename(input_archive))[0]
                    unique_name = f"{parent}_{base_name}{ext}"
                    destination_path = os.path.join(target_directory, unique_name)
                    shutil.move(file_path, destination_path)
                    found_files.add(destination_path)
                    logging.info(f"Extracted file: {destination_path}")

    if not found_files:
        logging.warning("No files found matching the specified extensions.")

    return found_files


def parse_pnadc_sas_script(file_path):
    """Parse a ``SAS`` script file to extract column names and specifications.
    
    Parameters
    ----------
    file_path : str
        The path to the ``SAS`` script file.
    
    Returns
    -------
    tuple
        A tuple containing:
        - A list of column names.
        - A list of tuples representing column specifications (start, end).
    """
    with open(file_path, 'r', encoding='latin-1') as file:
        content = file.read()

    # Extract column names
    colnames = re.findall(r'@\d+\s+(\w+)\s+', content)

    # Extract column specifications (start and end positions)
    colspecs = []
    for match in re.finditer(r'@(\d+)\s+\w+\s+[\$\.\d]+', content):
        start = int(match.group(1)) - 1  # Convert to 0-based index
        next_match = re.search(r'@(\d+)\s+\w+\s+[\$\.\d]+', content[match.end():])
        if next_match:
            end = int(next_match.group(1)) - 1
        else:
            # If no next match, assume the column ends at the end of the line
            end = start + 1  # Default to 1 character width
        colspecs.append((start, end))

    return colnames, colspecs
