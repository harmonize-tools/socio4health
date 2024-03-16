import pandas as pd


class DataInfo:
    def __init__(self, file_path=None, url=None, data=None):
        self._file_path = file_path
        self._url = url
        self._data = data

    def __str__(self):
        DataInfo_dict = {"file_path": self._file_path, "url": self._url}
        return str(DataInfo_dict)

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
