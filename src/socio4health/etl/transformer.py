import pandas as pd
import os
from pandas import DataFrame
import pyreadstat


class Transformer:
    """
    A class used to transform data into a Parquet file, with column and dtype options.

    Attributes:
        file_path (str): The path to the source file.
        output_path (str): The path to save the output Parquet file.
        columns (list): A list of columns to select.
        dTypes (dict): A dictionary to map columns to specific data types.
    """

    def __init__(self, file_path: str, output_path: str, columns=None, dTypes=None):
        self.file_path = file_path
        self.output_path = output_path
        self.columns = columns if columns else []
        self.dTypes = dTypes if dTypes else {}

    def _read_csv(self) -> DataFrame:
        """Reads CSV file, optionally with specified columns and dtypes."""
        return pd.read_csv(
            self.file_path,
            sep=r'[,;]',
            usecols=self.columns if self.columns else None,
            dtype=self.dTypes if self.dTypes else None
        )

    def _read_txt(self) -> DataFrame:
        """Reads TXT file, optionally with specified columns and dtypes."""
        return pd.read_table(
            self.file_path,
            sep=r'[,;]',
            usecols=self.columns if self.columns else None,
            dtype=self.dTypes if self.dTypes else None
        )

    def _read_excel(self) -> DataFrame:
        """Reads Excel file, optionally with specified columns and dtypes."""
        start_row = self._find_header_row()
        try:
            df = pd.read_excel(
                self.file_path,
                engine='openpyxl',
                skiprows=start_row,
                usecols=self.columns if self.columns else None,
                dtype=self.dTypes if self.dTypes else None
            )
        except Exception as e:
            raise ValueError(f'Error reading Excel file: {str(e)}')
        return df

    def _find_header_row(self) -> int:
        """Finds the header row in Excel files."""
        for i in range(20):  # Adjust range as needed
            df = pd.read_excel(self.file_path, engine='openpyxl', nrows=1, skiprows=i)
            if not df.empty and not df.columns.str.contains('Unnamed').any():
                return i
        raise ValueError('Valid header not found in the first 20 rows')

    def _read_sav(self) -> DataFrame:
        """Reads SAV file (SPSS format) with optional column and dtype handling."""
        df, meta = pyreadstat.read_sav(self.file_path)
        if self.columns:
            df = df[self.columns]  # Select only the required columns
        if self.dTypes:
            df = df.astype(self.dTypes)  # Apply the specified data types
        return df

    def transform_and_save(self) -> None:
        """
        Reads a file (CSV, TXT, XLSX, or SAV) with column and dtype filtering,
        and saves the transformed data to a Parquet file.

        Returns:
            None
        """
        _, file_extension = os.path.splitext(self.file_path)
        df = None
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

            # Create the output directory if it doesn't exist
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

            # Save DataFrame to Parquet
            df.to_parquet(self.output_path, index=False)

        except pd.errors.ParserError:
            raise ValueError('Error parsing the file, please check the content.')
        except FileNotFoundError:
            raise ValueError(f'File not found: {self.file_path}')
        except Exception as e:
            raise ValueError(f'An error occurred: {str(e)}')
