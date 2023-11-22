import pandas as pd
import os

from pandas import DataFrame


class Transformer:
    """ Data Transformer class
   """

    def __init__(self, file_path, db_file, table_name, headers=None):
        self.file_path = file_path
        self.db_file = db_file
        self.table_name = table_name
        self.headers = headers

    def transform_data(self) -> DataFrame:
        """
       Reads a file (CSV, TXT, or XLSX), selects specified columns, and writes the transformed data
       to a SQLite database.

       Args:
           file_name (str): Path to the file (CSV, TXT, or XLSX)
           in_col_names (list[str]): (Optional) List of column names to be selected from the file
           db_file (str): Path to the SQLite database file
           table_name (str): Name of the table in the SQLite database where the data will be written
           has_headers (bool): Boolean to indicate if the file has headers

       Returns:
           Dataframe: Dataframe of the trasformed file
       """
        # Get file extension
        _, file_extension = os.path.splitext(self.file_path)

        # Depending on the file extension, read the file using the appropriate pandas function
        if file_extension == '.csv':
            df = pd.read_csv(self.file_path, usecols=self.headers, header=0 if self.headers is None else None)
        elif file_extension == '.txt':
            df = pd.read_table(self.file_path, usecols=self.headers, header=0 if self.headers is None else None)
        elif file_extension == '.xlsx':
            df = pd.read_excel(self.file_path, usecols=self.headers, header=0 if self.headers is None else None)
        else:
            raise ValueError(f'Unsupported file type: {file_extension}')

        return df
