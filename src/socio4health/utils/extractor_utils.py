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
    """Run the Scrapy spider to extract data from the given URL."""
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
    """Download a file from the specified URL and save it to the given directory."""
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
    """Extract files from a compressed archive and return the paths of the extracted files."""
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
                    unique_name = f"{base_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
                    destination_path = os.path.join(target_directory, unique_name)
                    shutil.move(file_path, destination_path)
                    found_files.add(destination_path)
                    logging.info(f"Extracted file: {destination_path}")

    if not found_files:
        logging.warning("No files found matching the specified extensions.")

    return found_files


def parse_pnadc_sas_script(file_path):
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