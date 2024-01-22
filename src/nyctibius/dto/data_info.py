import pandas as pd


class DataInfo:
    def __init__(self, file_path=None, name=None, description=None, url=None, data=None):
        self._file_path = file_path
        self._name = name
        self._description = description
        self._url = url
        self._data = data

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, file_path):
        if file_path is not None and not isinstance(file_path, str):
            raise TypeError('file_path must be a String')
        try:
            self._file_path = file_path
        except Exception as e:
            raise Exception(f'Error setting file_path: {e}')

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if name is not None and not isinstance(name, str):
            raise TypeError('name must be a String')
        try:
            self._file_path = name
        except Exception as e:
            raise Exception(f'Error setting name: {e}')

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        if description is not None and not isinstance(description, str):
            raise TypeError('description must be a string')
        try:
            self._description = description
        except Exception as e:
            raise Exception(f'Error setting description: {e}')

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        if url is not None and not isinstance(url, str):
            raise TypeError('url must be a string')
        try:
            self._url = url
        except Exception as e:
            raise Exception(f'Error setting url: {e}')

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if data is not None and not isinstance(data, pd.DataFrame):
            raise TypeError('data must be a pandas DataFrame')
        try:
            self._data = data
        except Exception as e:
            raise Exception(f'Error setting data: {e}')
