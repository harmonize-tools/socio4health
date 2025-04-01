import logging
import pandas as pd



class Translator:
    def __init__(self, dictionary: pd.DataFrame = None, raw_dataframes: list | pd.DataFrame = None,
                 key_column: str = None, merge: bool = False, translated_dataframes: list = None):
        self._dictionary = dictionary
        self._raw_dataframes = raw_dataframes if isinstance(raw_dataframes, list) else [raw_dataframes]
        self._key_column = key_column
        self._merge = merge
        self._translated_dataframes = translated_dataframes

    @property
    def dictionary(self):
        return self._dictionary

    @dictionary.setter
    def dictionary(self, dictionary: pd.DataFrame):
        self._dictionary = dictionary

    @property
    def raw_dataframes(self):
        return self._raw_dataframes

    @raw_dataframes.setter
    def raw_dataframes(self, raw_dataframes: list | pd.DataFrame):
        self._raw_dataframes = raw_dataframes if isinstance(raw_dataframes, list) else [raw_dataframes]

    @property
    def key_column(self):
        return self._key_column

    @key_column.setter
    def key_column(self, key_column: str):
        self._key_column = key_column

    @property
    def merge(self):
        return self._merge

    @merge.setter
    def merge(self, merge: bool):
        self._merge = merge

    @property
    def translated_dataframes(self):
        return self._translated_dataframes

    @translated_dataframes.setter
    def translated_dataframes(self, translated_dataframes: list):
        self._translated_dataframes = translated_dataframes
