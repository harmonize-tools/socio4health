import os
import csv
import pathlib
import urllib
import requests


def get_csv_delimiter(file_path):
    """Get delimiter for a csv file with a given path.

    Args:
        file_path (str): Path to csv file

    Returns:
        delimiter: Delimiter of csv file
    """
    with open(file_path, "rb") as file:
        first_line = file.readline().decode()
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(first_line)
    return dialect.delimiter


def get_character_distance(character: str) -> int:
    """Method to convert a excel column into its corresponding index.

    Args:
        character (str): Excel column letters

    Returns:
        int: returns the index of the column.
    """
    num = 0
    for c in character:
        if c.isalpha():
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num


def is_url(url: str):
    """Determines if a input string is a valid URL address.

    Args:
        url (str): Input to evaluate

    Returns:
        bool: True if the string is a URL address
    """
    return urllib.parse.urlparse(url).scheme in ('http', 'https')


def url_exists(path: str):
    """Determines if a file exists in a given remote path (URL)

    Args:
        path (str): URL to a given file

    Returns:
        bool: True if it exists
    """
    req = requests.head(path)
    return req.status_code == 200


def get_path_filename_noext(path: str) -> str:
    """Return the file name with no extension from a given path.

    Args:
        path (str): Valid path

    Returns:
        str: Filename with no extension
    """
    return os.path.splitext(os.path.basename(path))[0]


def get_path_extension(path: str) -> str:
    """Return the extension from a given path.

    Args:
        path (str): Valid path

    Returns:
        str: Extension
    """
    extension = pathlib.Path(path).suffix
    return extension.upper().replace('.', '')


#################################################

def read_csv_file(file_path: str, columns):
    data = []
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file, fieldnames=columns)
        for row in csv_reader:
            data.append(row)
    return data
