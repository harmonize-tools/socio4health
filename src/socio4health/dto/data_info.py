import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


class DataInfo:
    """
    A class to manage data-related information including file paths, URLs, and DataFrames.

    Attributes:
        _file_path (str): The path to the data file.
        _url (str): The URL for data access.
        _country (str): The country of the data.
        _year (int): The year of the data.
        _data_source_type (str): The type of data source
    """

    def __init__(self, file_path=None, url=None, country=None, year=None, data_source_type=None):
        self._file_path = file_path
        self._url = url
        self._country = country
        self._year = year
        self._data_source_type = data_source_type

    def __str__(self):
        """Return a string representation of the DataInfo object."""
        DataInfo_dict = {
            "file_path": self.file_path,
            "url": self.url,
            "country": self.country,
            "year": self.year,
            "data_source_type": self.data_source_type
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

    @property
    def country(self):
        """Get the country."""
        return self._country

    @country.setter
    def country(self, country):
        """Set the country with validation."""
        if country is not None and not isinstance(country, str):
            raise TypeError('country must be a string')
        if country and not country.strip():
            raise ValueError('country cannot be an empty string')
        self._country = country

    @property
    def year(self):
        """Get the year."""
        return self._year

    @year.setter
    def year(self, year):
        """Set the year with validation."""
        if year is not None and not isinstance(year, int):
            raise TypeError('year must be an integer')
        if year and year < 0:
            raise ValueError('year cannot be negative')
        self._year = year

    @property
    def data_source_type(self):
        """Get the type."""
        return self._data_source_type

    @data_source_type.setter
    def data_source_type(self, data_source_type):
        """Set data source type with validation."""
        if data_source_type is not None and not isinstance(data_source_type, str):
            raise TypeError('type must be a string')
        if data_source_type and not data_source_type.strip():
            raise ValueError('type cannot be an empty string')
        self._data_source_type = data_source_type
