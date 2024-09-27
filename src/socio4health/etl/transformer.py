import pandas as pd
import os
from pandas import DataFrame
import pyreadstat
import re


class Transformer:
    """
    A class used to transform data into a Parquet file.

    Attributes:
        file_path (str): The path to the source file.
        output_path (str): The path to save the output Parquet file.
    """

    def __init__(self, file_path: str, output_path: str):
        self.file_path = file_path
        self.output_path = output_path

    def _read_csv(self) -> DataFrame:
        return pd.read_csv(self.file_path, sep=r'[,;]', engine='python')

    def _read_txt(self) -> DataFrame:
        return pd.read_table(self.file_path, sep=r'[,;]', engine='python')

    def _read_excel(self) -> DataFrame:
        start_row = self._find_header_row()
        try:
            df = pd.read_excel(self.file_path, engine='openpyxl', skiprows=start_row)
        except Exception as e:
            raise ValueError(f'Error reading Excel file: {str(e)}')
        return df

    def _find_header_row(self) -> int:
        for i in range(20):  # Adjust range as needed
            df = pd.read_excel(self.file_path, engine='openpyxl', nrows=1, skiprows=i)
            if not df.empty and not df.columns.str.contains('Unnamed').any():
                return i
        raise ValueError('Valid header not found in the first 20 rows')

    def _read_sav(self) -> DataFrame:
        df, meta = pyreadstat.read_sav(self.file_path)
        return df

    def transform_and_save(self) -> None:
        """
        Reads a file (CSV, TXT, XLSX, or SAV) and saves the transformed data to a Parquet file.

        Returns:
            None
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

            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            df.to_parquet(self.output_path, index=False)
        except pd.errors.ParserError:
            raise ValueError('Error parsing the file, please check the content.')
        except FileNotFoundError:
            raise ValueError(f'File not found: {self.file_path}')
        except Exception as e:
            raise ValueError(f'An error occurred: {str(e)}')
