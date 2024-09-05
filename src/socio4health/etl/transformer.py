import pandas as pd
import os
from pandas import DataFrame
import pyreadstat


class Transformer:
    """
    A class used to transform data into a Python Dataframe.

    Attributes:
        file_path (str): The path to the source file.
        db_file (str): The path to the SQLite database file.
    """

    def __init__(self, file_path, db_file):
        self.file_path = file_path
        self.db_file = db_file

    def _read_csv(self):
        return pd.read_csv(self.file_path, sep='[,|;]', engine='python')

    def _read_txt(self):
        return pd.read_table(self.file_path, sep='[,|;]', engine='python')

    def _read_excel(self):
        df = None
        start_row = self._find_header_row()
        try:
            df = pd.read_excel(self.file_path, engine='openpyxl', skiprows=start_row)
        except Exception as e:
            raise ValueError(f'Error reading Excel file: {str(e)}')
        return df

    def _find_header_row(self):
        for i in range(20):  # Adjust range as needed
            df = pd.read_excel(self.file_path, engine='openpyxl', nrows=1, skiprows=i)
            if not df.empty and not df.columns.str.contains('Unnamed').any():
                return i
        raise ValueError('Valid header not found in the first 20 rows')  # Adjust error message as needed

    def _read_sav(self):
        df, meta = pyreadstat.read_sav(self.file_path)
        return df

    def transform_data(self) -> DataFrame:
        """
        Reads a file (CSV, TXT, or XLSX), selects specified columns, and writes the transformed data
        to a SQLite database.

        Returns:
            Dataframe: Dataframe of the transformed file
        """
        _, file_extension = os.path.splitext(self.file_path)
        try:
            if file_extension.lower() == '.csv':
                df = self._read_csv()
            elif file_extension.lower() == '.txt':
                df = self._read_txt()
            elif file_extension.lower() in ['.xlsx', '.xls']:
                df = self._read_excel()
            elif file_extension.lower() == '.sav':
                df = self._read_sav()
            else:
                raise ValueError(f'Unsupported file type: {file_extension}')
            return df
        except Exception as e:
            raise