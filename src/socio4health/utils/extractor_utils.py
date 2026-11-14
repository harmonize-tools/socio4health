
import pyzipper
import zipfile
import shutil
import tempfile
import tarfile
import py7zr
import os
import requests
import time
import hashlib
import multiprocessing
import logging
from socio4health.utils.log import logger
from importlib import import_module
from socio4health.utils.deps import import_optional
import zipfile
import shutil
import tempfile
import tarfile
import py7zr
import os
import requests
import hashlib
import multiprocessing
import logging

# Configure logging


def _run_spider_in_process(url, depth, down_ext, key_words):
    """Internal function to run spider in a separate process"""
    # Configure logging for the subprocess
    logging.getLogger('scrapy').propagate = False
    logging.getLogger('scrapy').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    # Import scrapy only when running the spider (lazy)
    scrapy = import_optional('scrapy', extra='scraping')
    from scrapy.crawler import CrawlerProcess
    # Import local spider class
    spider_mod = import_module('socio4health.utils.standard_spider')
    StandardSpider = getattr(spider_mod, 'StandardSpider')

    process = CrawlerProcess({
        'LOG_LEVEL': 'CRITICAL',
        'LOG_ENABLED': False,
        'TWISTED_REACTOR_ENABLED': False,
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
    })
    process.crawl(StandardSpider, url=url, depth=depth, down_ext=down_ext, key_words=key_words)
    process.start(stop_after_crawl=True)


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
    bool
        True if spider completed successfully, False otherwise
    """
    try:
        # Create and run spider in a separate process
        spider_process = multiprocessing.Process(
            target=_run_spider_in_process,
            args=(url, depth, down_ext, key_words)
        )

        spider_process.start()
        spider_process.join()  # Wait for the process to finish

        # Check if process completed successfully
        if spider_process.exitcode == 0:
            logger.debug(f"Spider completed successfully for URL: {url}")
            return True
        else:
            logger.error(f"Spider process failed with exit code: {spider_process.exitcode}")
            return False

    except Exception as e:
        logger.error(f"Error running spider: {str(e)}")
        return False


DOWNLOAD_TIMEOUT = (15, 60)  # seconds: (connect, max wait between two chunks of data)
DOWNLOAD_RETRIES = 3
_CHUNK_SIZE = 1 << 20  # 1 MiB


def download_request(url, filename, download_dir, on_progress=None, retries=DOWNLOAD_RETRIES,
                     timeout=DOWNLOAD_TIMEOUT):
    """Download a file from the specified ``URL`` and save it to the given directory.

    The file is written as ``<filename>.part`` and renamed only when complete, so an interrupted
    download never leaves a truncated file behind. If the file already exists with the size reported
    by the server, it is not downloaded again.

    Parameters
    ----------
    url : str
        The ``URL`` of the file to download.
    filename : str
        The name to save the downloaded file.
    download_dir : str
        The directory where the file will be saved.
    on_progress : callable, optional
        Called as ``on_progress(bytes_done, bytes_total)`` while downloading (``bytes_total`` may be ``None``).
    retries : int, optional
        Number of attempts before giving up. Default is 3.
    timeout : tuple, optional
        ``(connect, read)`` timeouts in seconds. The download fails if the server sends nothing for
        ``read`` seconds, instead of waiting forever.

    Returns
    -------
    str
        The path to the downloaded file, or ``None`` if the download failed.

    """
    filepath = os.path.join(download_dir, filename)
    part_path = filepath + ".part"
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                total = int(response.headers.get("Content-Length", 0)) or None

                if total and os.path.exists(filepath) and os.path.getsize(filepath) == total:
                    logger.debug(f"Already downloaded, skipping: {filename}")
                    if on_progress:
                        on_progress(total, total)
                    return filepath

                done = 0
                with open(part_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                        if chunk:
                            file.write(chunk)
                            done += len(chunk)
                            if on_progress:
                                on_progress(done, total)

            if total and done != total:
                raise requests.exceptions.ContentDecodingError(
                    f"incomplete download ({done} of {total} bytes)")
            os.replace(part_path, filepath)
            logger.debug(f"Successfully downloaded: {filename}")
            return filepath

        except (requests.exceptions.RequestException, OSError) as e:
            last_error = e
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            if attempt < retries:
                wait = 2 ** attempt
                logger.warning(f"Download of {filename} failed (attempt {attempt}/{retries}): {e}. "
                               f"Retrying in {wait}s...")
                time.sleep(wait)

    logger.error(f"Error downloading {filename} from {url} after {retries} attempts: {last_error}")
    return None


def compressed2files(input_archive, target_directory, down_ext, current_depth=0, max_depth=5, found_files=None,
                     _archive_key=None):
    """Extract files from a compressed archive and return the paths of the extracted files.

    Only the members whose extension is in ``down_ext`` (and nested archives) are extracted; other
    members, such as the same data in other formats, are never written to disk. Members that were
    already extracted with the same size are not extracted again.

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

    logger.debug(f"Processing (depth {current_depth}): {os.path.basename(input_archive)}")

    if current_depth > max_depth:
        logger.warning(f"Max depth {max_depth} reached")
        return set()

    # Name prefix of the extracted files. For nested archives it is derived from the outer archive and
    # the member name (not from the temporary path), so names stay the same between runs.
    archive_key = _archive_key or input_archive
    archive_hash = hashlib.md5(archive_key.encode()).hexdigest()[:8]
    wanted_ext = {ext.lower() for ext in down_ext}
    found_files = set()
    all_members = []

    def is_nested(name):
        return name.lower().endswith(_NESTED_ARCHIVE_EXT)

    def is_wanted(name):
        return is_nested(name) or os.path.splitext(name)[1].lower() in wanted_ext

    def destination(member_name):
        return os.path.join(target_directory, f"{archive_hash}_{member_name.strip('/').replace('/', '_')}")

    def handle_member(name, size, open_member, temp_dir):
        """Extract a single member, unless it is not wanted or it is already extracted."""
        if is_nested(name):
            nested_path = os.path.join(temp_dir, os.path.basename(name))
            with open_member() as src, open(nested_path, 'wb') as out:
                shutil.copyfileobj(src, out, _CHUNK_SIZE)
            found_files.update(compressed2files(nested_path, target_directory, down_ext, current_depth + 1,
                                                max_depth, _archive_key=f"{archive_key}/{name}"))
            os.remove(nested_path)
            return
        dest_path = destination(name)
        if os.path.exists(dest_path) and os.path.getsize(dest_path) == size:
            logger.debug(f"Already extracted, skipping: {os.path.basename(dest_path)}")
        else:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            part_path = dest_path + ".part"
            with open_member() as src, open(part_path, 'wb') as out:
                shutil.copyfileobj(src, out, _CHUNK_SIZE)
            os.replace(part_path, dest_path)
            logger.debug(f"Extracted: {os.path.basename(dest_path)}")
        found_files.add(dest_path)

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            if zipfile.is_zipfile(input_archive):
                try:
                    _extract_zip_members(zipfile.ZipFile, input_archive, is_wanted, handle_member, temp_dir,
                                         all_members)
                except NotImplementedError as e:
                    # e.g. Deflate64, which the standard library cannot decompress
                    logger.warning(f"zipfile failed for {input_archive}: {e}. Trying pyzipper fallback.")
                    try:
                        all_members.clear()
                        _extract_zip_members(pyzipper.ZipFile, input_archive, is_wanted, handle_member, temp_dir,
                                             all_members)
                    except Exception as e2:
                        logger.error(f"pyzipper extraction failed for {input_archive}: {e2}")
                        return set()
            elif tarfile.is_tarfile(input_archive):
                with tarfile.open(input_archive, 'r:*') as tar_ref:
                    for member in tar_ref.getmembers():
                        if not member.isfile():
                            continue
                        all_members.append(member.name)
                        if is_wanted(member.name):
                            handle_member(member.name, member.size,
                                          lambda m=member: tar_ref.extractfile(m), temp_dir)
            elif input_archive.endswith('.7z'):
                with py7zr.SevenZipFile(input_archive, mode='r') as z_ref:
                    entries = [e for e in z_ref.list() if not e.is_directory]
                all_members.extend(e.filename for e in entries)
                targets = [e.filename for e in entries if is_wanted(e.filename)
                           and (is_nested(e.filename) or not (os.path.exists(destination(e.filename))
                                and os.path.getsize(destination(e.filename)) == e.uncompressed))]
                if targets:
                    with py7zr.SevenZipFile(input_archive, mode='r') as z_ref:
                        z_ref.extract(path=temp_dir, targets=targets)
                for e in entries:
                    if is_wanted(e.filename):
                        extracted = os.path.join(temp_dir, e.filename)
                        handle_member(e.filename, e.uncompressed,
                                      lambda p=extracted: open(p, 'rb'), temp_dir)
            else:
                logger.error(f"Unsupported format: {input_archive}")
                return set()

        except Exception as e:
            logger.error(f"Failed to process {input_archive}: {str(e)}", exc_info=True)
            return set()

    if not found_files:
        logger.warning(f"No files matching {sorted(wanted_ext)} in {os.path.basename(input_archive)}")
        for name in all_members:
            logger.debug(f"  Found: {name}")

    return found_files


_NESTED_ARCHIVE_EXT = ('.zip', '.7z', '.tar', '.gz', '.tgz')


def _extract_zip_members(zip_class, input_archive, is_wanted, handle_member, temp_dir, all_members):
    with zip_class(input_archive, 'r') as zip_ref:
        for info in zip_ref.infolist():
            if info.is_dir():
                continue
            all_members.append(info.filename)
            if is_wanted(info.filename):
                handle_member(info.filename, info.file_size, lambda i=info: zip_ref.open(i), temp_dir)

def create_unique_path(archive_path, filename, target_dir):
    """Generate unique destination path"""
    archive_name = os.path.splitext(os.path.basename(archive_path))[0]
    base, ext = os.path.splitext(filename)
    unique_name = f"{archive_name}_{base}{ext}"
    return os.path.join(target_dir, unique_name)

def s4h_parse_fwf_dict(dict_df):
    """Parse a fixed-width format dictionary stored in a pandas DataFrame.

    The DataFrame must contain at least the following columns:
    ``variable_name`` and ``initial_position``. Either ``size`` or
    ``final_position`` must be present to compute column spans.

    Parameters
    ----------
    dict_df : pandas.DataFrame
        Dictionary table describing fixed-width columns.

    Returns
    -------
    tuple
        ``(colnames, colspecs)`` where ``colnames`` is a list of column
        names and ``colspecs`` is a list of ``(start, end)`` integer tuples
        suitable for use with ``pandas.read_fwf`` (0-based, end exclusive).

    Raises
    ------
    ValueError
        If required columns are missing.
    """

    if not 'variable_name' in dict_df.columns:
        raise ValueError("No column names found in the dictionary DataFrame.")
    if not 'initial_position' in dict_df.columns:
        raise ValueError("No initial positions found in the dictionary DataFrame.")
    if not 'size' in dict_df.columns and not 'final_position' in dict_df.columns:
        raise ValueError("No sizes or final postions found in the dictionary DataFrame.")

    # Extract column names
    colnames = dict_df['variable_name'].tolist()
    colspecs = []
    if 'final_position' in dict_df.columns:
        for i,val in dict_df['initial_position'].items():
                start = int(val - 1)
                end = int(dict_df['final_position'][i])
                colspecs.append((start, end))
    else:
        size = dict_df['size'].tolist()
        for i,val in dict_df['initial_position'].items():
            start = int(val - 1)
            end = int(start + size[i])
            colspecs.append((start, end))

    return colnames, colspecs


def s4h_parse_ibge_layout(dictionary, variables=None):
    """Read the column layout of a fixed-width file from an IBGE dictionary (PNAD, PNAD Contínua).

    IBGE dictionaries (``dicionario_PNADC_microdados_<year>_visita1.xls`` and similar) list each variable
    on a row as ``initial position | size | code | question | description | categories...``. The
    layout changes between years, so read the dictionary published with each year's file instead of
    reusing one layout for every year.

    Parameters
    ----------
    dictionary : str, Path or pandas.DataFrame
        Path to the dictionary (``.xls``/``.xlsx``; ``.xls`` needs ``xlrd``) or its first sheet read with
        ``header=None``.
    variables : iterable of str, optional
        Only return these variables (case-insensitive). Reading fewer columns is much faster.

    Returns
    -------
    tuple
        ``(colnames, colspecs)`` for ``Extractor(is_fwf=True, ...)`` / ``pandas.read_fwf``: variable codes
        and ``(start, end)`` tuples (0-based, end exclusive).
    """
    import re
    import pandas as pd

    df = dictionary if isinstance(dictionary, pd.DataFrame) else pd.read_excel(dictionary, header=None, dtype=str)
    wanted = {str(v).strip().upper() for v in variables} if variables is not None else None
    colnames, colspecs, seen = [], [], set()
    for row in df.fillna("").astype(str).itertuples(index=False):
        cells = [c.strip() for c in row]
        for j in range(len(cells) - 2):
            start, size, code = cells[j], cells[j + 1], cells[j + 2]
            if start.isdigit() and size.isdigit() and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,11}", code):
                key = code.upper()
                if key not in seen and (wanted is None or key in wanted):
                    seen.add(key)
                    colnames.append(code)
                    colspecs.append((int(start) - 1, int(start) - 1 + int(size)))
                break
    if not colnames:
        raise ValueError("No variables found in the IBGE dictionary.")
    return colnames, colspecs
