import pandas as pd
import os
from pandas import DataFrame
import pyreadstat


class Transformer:
    """ Data Transformer class
   """

    def __init__(self, file_path, db_file):
        self.file_path = file_path
        self.db_file = db_file


    def transform_data(self, headers=None) -> DataFrame:
        """
       Reads a file (CSV, TXT, or XLSX), selects specified columns, and writes the transformed data
       to a SQLite database.

       Args:
           headers (List[str]): Path to the file (CSV, TXT, or XLSX)

       Returns:
           Dataframe: Dataframe of the transformed file
       """
        # Get file extension
        _, file_extension = os.path.splitext(self.file_path)
        # TODO delete file when saved df
        # Depending on the file extension, read the file using the appropriate pandas function
        if file_extension.lower() == '.csv':
            df = pd.read_csv(self.file_path, usecols=headers, header=0 if headers is None else None)
        elif file_extension.lower() == '.txt':
            df = pd.read_table(self.file_path, usecols=headers, header=0 if headers is None else None)
        elif file_extension.lower() == '.xlsx' or file_extension.lower() == '.xls':
            df = pd.read_excel(self.file_path, usecols=headers, header=0 if headers is None else None)
        elif file_extension.lower() == '.sav':
            df, meta = pyreadstat.read_sav(self.file_path, usecols=headers)
        else:
            raise ValueError(f'Unsupported file type: {file_extension}')
        return df
