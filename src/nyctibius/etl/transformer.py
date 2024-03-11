import pandas as pd
import os
from pandas import DataFrame
import pyreadstat

from nyctibius.utils.utils import cell_to_indices


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
            df = pd.read_csv(self.file_path, sep='[,|;]', engine='python', usecols=headers, header=0 if headers is None else None)
        elif file_extension.lower() == '.txt':
            df = pd.read_table(self.file_path, usecols=headers, header=0 if headers is None else None)
        elif file_extension.lower() == '.xlsx' or file_extension.lower() == '.xls':
            while True:
                cell = input("Enter the start cell (e.g., A1): ")
                start_row, start_column = cell_to_indices(cell)
                df = pd.read_excel(self.file_path, usecols=range(start_column, None), skiprows=start_row,
                                   header=0 if headers is None else None)
                print(df.iloc[0])
                print(df.iloc[1])
                confirmation = input("Is this the correct starting row that includes column names? (yes/no): ")
                if confirmation.lower() == 'yes':
                    break
        elif file_extension.lower() == '.sav':
            df, meta = pyreadstat.read_sav(self.file_path, usecols=headers)
        else:
            raise ValueError(f'Unsupported file type: {file_extension}')
        return df
