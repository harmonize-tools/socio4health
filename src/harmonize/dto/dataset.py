import pandas as pd


class Dataset:
    def __init__(self, data, name, description, url):
        self._data = data
        self._name = name
        self._description = description
        self._url = url

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if not isinstance(data, pd.DataFrame):
            raise TypeError('data must be a pandas DataFrame')
        try:
            self._data = data
        except Exception as e:
            raise Exception(f'Error setting data: {e}')

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        try:
            self._name = name
        except Exception as e:
            raise Exception(f'Error setting name: {e}')

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
