import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


class DataInfo:
    """
    A class to manage data-related information including file paths, URLs, and DataFrames.

    Attributes:
        file_path (str): The path to the data file.
        url (str): The URL for data access.
        selected_columns (list): List of selected column names
        info (str): Information about the DataFrame.
    """

    def __init__(self, file_path=None, url=None):
        self._file_path = file_path
        self._url = url

    def __str__(self):
        """Return a string representation of the DataInfo object."""
        DataInfo_dict = {
            "file_path": self.file_path,
            "url": self.url
        }
        return str(DataInfo_dict)

    @property
    def file_path(self):
        """Get the file path."""
        return self._file_path

    @file_path.setter
    def file_path(self, file_path):
        """Set the file path with validation."""
        if file_path is not None and not isinstance(file_path, str):
            raise TypeError('file_path must be a string')
        if file_path and not file_path.strip():
            raise ValueError('file_path cannot be an empty string')
        self._file_path = file_path

    @property
    def url(self):
        """Get the URL."""
        return self._url

    @url.setter
    def url(self, url):
        """Set the URL with validation."""
        if url is not None and not isinstance(url, str):
            raise TypeError('url must be a string')
        if url and not url.strip():
            raise ValueError('url cannot be an empty string')
        self._url = url
