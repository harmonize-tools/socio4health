import logging

import pandas as pd
import os
from pandas import DataFrame
import pyreadstat

from socio4health.dict.translator import Translator


class Transformer:
    """
    A class used to transform data into a Parquet file, with column and dtype options.

    Attributes:
        output_path (str): The path to save the output Parquet file.
        data_info (DataInfo): Data information object.
        selected_columns (list): A list of columns to select.
    """

    def __init__(self, output_path: str, data_info=None):
        self.output_path = output_path
        self.original_columns = []
        self.selected_columns = []
        self.data_info = data_info

    @property
    def selected_columns(self):
        return self._selected_columns

    @selected_columns.setter
    def selected_columns(self, selected_columns):
        if not isinstance(selected_columns, list):
            raise ValueError("Columns must be a list")
        self._selected_columns = selected_columns

    def set_columns(self, selected_columns, harmonized=True, mapping_path=None):
        """
        Sets the selected columns by harmonizing them using the Translator.
        """
        if not isinstance(selected_columns, list):
            raise ValueError("Columns must be a list")
        if not harmonized:
            self.selected_columns = selected_columns
        else:
            translator = Translator(mapping_path, data_info=self.data_info)
            translated_columns = [translator.mapped_to_variable(col) for col in selected_columns]
            translated_columns = [col for col in translated_columns if col is not None]
            print(f"Selected columns: {selected_columns}")
            print(f"Translated columns: {translated_columns}")
            self.selected_columns = translated_columns

    def _read_csv(self, nrows=None) -> DataFrame:
        """Reads CSV file, optionally with specified columns, dtypes, and nrows."""
        return pd.read_csv(
            self.data_info.file_path,
            engine='python',
            sep=r'[,;]',
            usecols=self.selected_columns if self.selected_columns else None,
            nrows=nrows
        )

    def _read_txt(self, nrows=None) -> DataFrame:
        """Reads TXT file, optionally with specified columns, dtypes, and nrows."""
        return pd.read_table(
            self.data_info.file_path,
            engine='python',
            sep=r'[,;]',
            usecols=self.selected_columns if self.selected_columns else None,
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
                usecols=self.selected_columns if self.selected_columns else None,
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
        if self.selected_columns:
            df = df[self.selected_columns]
        return df

    def available_columns(self, harmonized=True, mapping_path=None) -> dict:
        """
        Fetches the available columns in the source file, depending on the file type,
        returning a mapping of translated columns to their original names.

        Args:
            harmonized (bool): Whether to return harmonized (translated) column names.
            mapping_path (str): Path to the mapping file for translation.

        Returns:
            dict: A dictionary mapping translated column names to their original names.
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

            available_columns = df.columns.tolist()
            if not self.original_columns:
                self.original_columns = available_columns

            column_mapping = {}  # Dictionary to hold the mapping

            if harmonized:
                translator = Translator(data_info=self.data_info)
                for col in available_columns:
                    #aqui es donde se hace la harmonizacion
                    translated_col = translator.variable_to_mapped(col)
                    if translated_col is not None:
                        column_mapping[translated_col] = col

            else:
                # If not harmonized, return original columns as keys
                column_mapping = {col: col for col in available_columns}

            return column_mapping

        except pd.errors.ParserError:
            raise ValueError('Error parsing the file, please check the content.')
        except FileNotFoundError:
            raise ValueError(f'File not found: {self.data_info.file_path}')
        except Exception as e:
            raise ValueError(f'An error occurred: {str(e)}')

    def selected_columns_CLI(self) -> None:
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
            self.selected_columns = selected_columns
        else:
            self.selected_columns = available_columns

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

            if not self.selected_columns:
                self.selected_columns = self.available_columns(harmonized=False)

            if self.output_path and not os.path.exists(self.output_path):
                os.makedirs(self.output_path, exist_ok=True)
                logging.info(f"Created output directory: {self.output_path}")

            parquet_file = os.path.join(self.output_path,
                                        f"{os.path.splitext(os.path.basename(self.data_info.file_path))[0]}.parquet")
            df[self.selected_columns].to_parquet(parquet_file, index=False)

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
