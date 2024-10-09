import logging

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
        data_info (DataInfo): Data information object.
        columns (list): A list of columns to select.
        dTypes (dict): A dictionary to map columns to specific data types.
    """

    def __init__(self, output_path: str, columns=None, data_info=None):
        self.output_path = output_path
        self.columns = columns if columns else []
        self.data_info = data_info

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns):
        if not isinstance(columns, list):
            raise ValueError("Columns must be a list")
        self._columns = columns

    def _read_csv(self, nrows=None) -> DataFrame:
        """Reads CSV file, optionally with specified columns, dtypes, and nrows."""
        return pd.read_csv(
            self.data_info.file_path,
            engine='python',
            sep=r'[,;]',
            usecols=self.columns if self.columns else None,
            nrows=nrows  # Adding nrows for preview purposes
        )

    def _read_txt(self, nrows=None) -> DataFrame:
        """Reads TXT file, optionally with specified columns, dtypes, and nrows."""
        return pd.read_table(
            self.data_info.file_path,
            engine='python',
            sep=r'[,;]',
            usecols=self.columns if self.columns else None,
            nrows=nrows
        )

    def _read_excel(self, nrows=None) -> DataFrame:
        """Reads Excel file, optionally with specified columns, dtypes, and nrows."""
        start_row = self._find_header_row()
        try:
            df = pd.read_excel(
                self.data_info.file_path,
                engine='openpyxl',
                skiprows=start_row,
                usecols=self.columns if self.columns else None,
                nrows=nrows
            )
        except Exception as e:
            raise ValueError(f'Error reading Excel file: {str(e)}')
        return df

    def _find_header_row(self) -> int:
        """Finds the header row in Excel files."""
        for i in range(20):  # Adjust range as needed
            df = pd.read_excel(self.data_info.file_path, engine='openpyxl', nrows=1, skiprows=i)
            if not df.empty and not df.columns.str.contains('Unnamed').any():
                return i
        raise ValueError('Valid header not found in the first 20 rows')

    def _read_sav(self, nrows=None) -> DataFrame:
        """Reads SAV file (SPSS format) with optional column and dtype handling."""
        df, meta = pyreadstat.read_sav(self.data_info.file_path, row_limit=nrows)
        if self.columns:
            df = df[self.columns]
        return df

    def available_columns(self) -> list:
        """
        Fetches the available columns in the source file, depending on the file type.

        Returns:
            list: List of available column names.
        """
        _, file_extension = os.path.splitext(self.data_info.file_path)
        df = None
        try:
            if file_extension.lower() == '.csv':
                df = self._read_csv(nrows=5)
            elif file_extension.lower() == '.txt':
                df = self._read_txt(nrows=5)
            elif file_extension.lower() in ['.xlsx', '.xls']:
                df = self._read_excel(nrows=5)
            elif file_extension.lower() == '.sav':
                df = self._read_sav(nrows=5)
            else:
                raise ValueError(f'Unsupported file type: {file_extension}')

            return df.columns.tolist()

        except pd.errors.ParserError:
            raise ValueError('Error parsing the file, please check the content.')
        except FileNotFoundError:
            raise ValueError(f'File not found: {self.data_info.file_path}')
        except Exception as e:
            raise ValueError(f'An error occurred: {str(e)}')

    def select_columns_CLI(self) -> None:
        """
        Prompts the user to select columns from the available columns in the source file.

        Returns:
            None: Updates the self.columns with user-selected columns or defaults to all columns.
        """
        available_columns = self.available_columns()
        print(f"Available columns: {available_columns}")
        selected = input("Enter columns to select, separated by commas (or press Enter to select all): ").strip()
        if selected:
            selected_columns = [col.strip() for col in selected.split(',')]
            invalid_columns = [col for col in selected_columns if col not in available_columns]
            if invalid_columns:
                raise ValueError(f"Invalid columns selected: {invalid_columns}")
            self.columns = selected_columns
        else:
            self.columns = available_columns

    def transform(self, delete_files=False) -> None:
        """
        Reads a file (CSV, TXT, XLSX, or SAV) with column and dtype filtering,
        and saves the transformed data to a Parquet file.

        If delete_files is True, deletes the input file after transformation.

        Returns:
            None
        """
        _, file_extension = os.path.splitext(self.data_info.file_path)
        logging.info("----------------------")
        logging.info("Transforming data...")
        try:
            # Read the file based on its type
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

            # Prompt user to select columns if not already provided
            if not self.columns:
                self.select_columns()

            # Ensure the output directory exists, only if the output path includes a directory
            output_dir = os.path.dirname(self.output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            # Save DataFrame to Parquet
            df[self.columns].to_parquet(self.output_path, index=False)

            # If delete_files is True, delete the original file
            if delete_files:
                try:
                    os.remove(self.data_info.file_path)
                    print(f"Deleted source file: {self.data_info.file_path}")
                except OSError as e:
                    print(f"Error deleting file {self.data_info.file_path}: {e}")

        except pd.errors.ParserError:
            raise ValueError('Error parsing the file, please check the content.')
        except FileNotFoundError:
            raise ValueError(f'File not found: {self.data_info.file_path}')
        except Exception as e:
            raise ValueError(f'An error occurred: {str(e)}')
