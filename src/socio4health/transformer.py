import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transformer:

    def __init__(self, dictionary: pd.DataFrame = None, raw_dataframes: list | pd.DataFrame = None,
                 key_column: str = None, merge: bool = False, translated_dataframes: list = None,
                 selected_columns: list = None):
        self._dictionary = dictionary
        self._raw_dataframes = raw_dataframes if isinstance(raw_dataframes, list) else [raw_dataframes]
        self._key_column = key_column
        self._merge = merge
        self._selected_columns = selected_columns
        self._translated_dataframes = translated_dataframes if isinstance(translated_dataframes, list) else [translated_dataframes]

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
    def translated_dataframes(self, translated_dataframes: list | pd.DataFrame):
        self._translated_dataframes = translated_dataframes if isinstance(translated_dataframes, list) else [translated_dataframes]

    def get_columns(self, dataframes: list[pd.DataFrame]) -> list[str]:
        unique_columns = set()
        for df in dataframes:
            unique_columns.update(df.columns)
        return list(unique_columns)

    def translate(self):
        logging.info("----------------------")
        logging.info("Starting data translation...")
        try:
            if self.dictionary is None:
                logging.error("No dictionary was provided for translation.")
                raise ValueError("No dictionary was provided for translation.")
            if self.raw_dataframes is None:
                logging.error("No raw dataframes were provided for translation.")
                raise ValueError("No raw dataframes were provided for translation.")
            if self.key_column is None:
                logging.error("No key column was provided for translation.")
                raise ValueError("No key column was provided for translation.")
            if self.merge:
                self._merge_dataframes()
            self._translate_dataframes()
            logging.info("Translation completed successfully.")
        except Exception as e:
            logging.error(f"Exception while translating data: {e}")
            raise ValueError(f"Translation failed: {str(e)}")
        return self.translated_dataframes