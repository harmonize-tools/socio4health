import pandas as pd


class Dataset:
    def __init__(self, file_path, description, url):
        self._file_path = file_path
        self._description = description
        self._url = url

    @property
    def file_path(self):
        return self.file_path

    @file_path.setter
    def data(self, file_path):
        if not isinstance(file_path, str):
            raise TypeError('data must be a String')
        try:
            self._file_path = file_path
        except Exception as e:
            raise Exception(f'Error setting data: {e}')

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        if not isinstance(description, str):
            raise TypeError('name must be a string')
        try:
            self._description = description
        except Exception as e:
            raise Exception(f'Error setting name: {e}')

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        if not isinstance(url, str):
            raise TypeError('name must be a string')
        try:
            self._url = url
        except Exception as e:
            raise Exception(f'Error setting name: {e}')
