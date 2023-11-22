import pandas as pd
import os

from pandas import DataFrame


class Transformer:
    """ Data Transformer class
    """

    def __init__(self, file_name, in_col_names, out_col_names, db_file, table_name):
        self.file_name = file_name
        self.in_col_names = in_col_names
        self.out_col_names = out_col_names
        self.db_file = db_file
        self.table_name = table_name

    def transform_data(self) -> DataFrame:

        """
        Reads a file (CSV, TXT, or XLSX), selects specified columns, renames them, and writes the transformed data 
        to a SQLite database.

        Args:
            file_name (str): Path to the file (CSV, TXT, or XLSX)
            in_col_names (list[str]): List of column names to be selected from the file
            out_col_names (list[str]): (Optional) List of new column names to be used in the DataFrame
            db_file (str): Path to the SQLite database file
            table_name (str): Name of the table in the SQLite database where the data will be written

        Returns:
            Dataframe: Dataframe of the trasformed file
        """

        # Get file extension
        _, file_extension = os.path.splitext(self.file_name)

        # Depending on the file extension, read the file using the appropriate pandas function
        if file_extension == '.csv':
            df = pd.read_csv(self.file_name, usecols=self.in_col_names, header=0)
        elif file_extension == '.txt':
            df = pd.read_table(self.file_name, usecols=self.in_col_names, header=0)
        elif file_extension == '.xlsx':
            df = pd.read_excel(self.file_name, usecols=self.in_col_names, header=0)
        else:
            raise ValueError(f'Unsupported file type: {file_extension}')

        # Rename the columns
        if self.out_col_names is not None:
            df.columns = self.out_col_names

        return df
