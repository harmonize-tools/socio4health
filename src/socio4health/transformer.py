import logging
import pandas as pd
import os
from socio4health.dict.translator import Translator

class Transformer:
    """
    A class used to transform data into a Parquet file, with column and dtype options.

    Attributes:
        output_path (str): The path to save the output Parquet file.
        dataframe (pd.DataFrame): DataFrame to be transformed.
        selected_columns (list): A list of columns to select.
    """

    def __init__(self, dataframe: pd.DataFrame, output_path: str):
        self.output_path = output_path
        self.dataframe = dataframe
        self.original_columns = dataframe.columns.tolist()
        self.selected_columns = []

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
            translator = Translator(mapping_path)
            translated_columns = [translator.mapped_to_variable(col) for col in selected_columns]
            translated_columns = [col for col in translated_columns if col is not None]
            print(f"Selected columns: {selected_columns}")
            print(f"Translated columns: {translated_columns}")
            self.selected_columns = translated_columns

    def available_columns(self, harmonized=True, mapping_path=None) -> dict:
        """
        Fetches the available columns in the DataFrame, returning a mapping of translated columns to their original names.

        Args:
            harmonized (bool): Whether to return harmonized (translated) column names.
            mapping_path (str): Path to the mapping file for translation.

        Returns:
            dict: A dictionary mapping translated column names to their original names.
        """
        available_columns = self.dataframe.columns.tolist()
        column_mapping = {}

        if harmonized:
            translator = Translator(mapping_path)
            for col in available_columns:
                translated_col = translator.variable_to_mapped(col)
                if translated_col is not None:
                    column_mapping[translated_col] = col
        else:
            column_mapping = {col: col for col in available_columns}

        return column_mapping

    def selected_columns_CLI(self) -> None:
        """
        Prompts the user to select columns from the available columns in the DataFrame.

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

    def transform_and_save(self, delete_files=False) -> None:
        """
        Transforms the DataFrame and saves it to a Parquet file.

        If delete_files is True, deletes the input file after transformation.

        Returns:
            None
        """
        logging.info("----------------------")
        logging.info("Transforming data...")

        if not self.selected_columns:
            self.selected_columns = self.available_columns(harmonized=False)

        if self.output_path and not os.path.exists(self.output_path):
            os.makedirs(self.output_path, exist_ok=True)
            logging.info(f"Created output directory: {self.output_path}")

        parquet_file = os.path.join(self.output_path, "transformed_data.parquet")
        self.dataframe[self.selected_columns].to_parquet(parquet_file, index=False)

        if delete_files:
            try:
                os.remove(self.dataframe.file_path)
                print(f"Deleted source file: {self.dataframe.file_path}")
            except OSError as e:
                print(f"Error deleting file {self.dataframe.file_path}: {e}")