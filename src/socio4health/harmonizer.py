import os
import shutil
from importlib.metadata import files
from typing import List
import pandas as pd
from tqdm import tqdm
import logging
from socio4health.extractor import Extractor
from socio4health.transformer import Transformer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Harmonizer:

    def __init__(self, dataframe: pd.DataFrame = None, extractor: Extractor = None, transformer: Transformer = None,
                 input_folder="data/input", output_folder="data/output", name=None, url=None, country=None, year=None,
                 data_source_type=None, is_aggregated=False):
        """
        Initialize the Harmonizer with a list of DataFrames.
        Args:
            dataframe (pd.DataFrame): DataFrame.
            extractor (Extractor): Extractor instance.
            transformer (Transformer): Transformer instance.
            input_folder (str): Input folder path.
            output_folder (str): Output folder path.
            name (str): Name of the dataset.
            url (str): URL of the dataset.
            country (str): Country of the dataset.
            year (int): Year of the dataset.
            data_source_type (str): Type of the data source.
            is_aggregated (bool): Flag indicating if the data is aggregated.
        """
        self.dataframes = dataframe
        self.extractor = extractor
        self.transformer = transformer
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.name = name
        self.url = url
        self.country = country
        self.year = year
        self.data_source_type = data_source_type
        self.is_aggregated = is_aggregated

    def set_extractor(self, extractor: Extractor):
        """
        Sets the extractor for the Harmonizer.

        Args:
            extractor (Extractor): The extractor to set.
        """
        self.extractor = extractor

    def set_transformer(self, transformer: Transformer):
        """
        Sets the transformer for the Harmonizer.

        Args:
            transformer (Transformer): The transformer to set.
        """
        self.transformer = transformer

    def extract(self, path=None, url=None, depth=0, down_ext=['.csv', '.xls', '.xlsx', ".txt", ".sav", ".zip"],
                download_dir="data/input", key_words=[], encoding='latin1', is_fwf=False, colnames=None, colspecs=None,
                delete_data_dir=False, sep=',') -> List[pd.DataFrame]:
        """
        Extract data based on the provided configuration.

        Args:
            path (str): Local path to extract files from.
            url (str): URL to download data from.
            depth (int): Depth for recursive directory search.
            down_ext (list): List of file extensions to download.
            download_dir (str): Directory to save downloaded files.
            key_words (list): Keywords to filter the files.
            encoding (str): Encoding of the files.
            is_fwf (bool): Whether the file is fixed-width formatted.
            colnames (list): Column names for fixed-width files.
            colspecs (list): Column specifications for fixed-width files.
            delete_data_dir (bool): Whether to delete the data directory after extraction.

        Returns:
            List[pd.DataFrame]: A list of extracted data as DataFrames.
        """
        logging.info("----------------------")
        logging.info("Extracting data...")

        if self.extractor is None:
            self.extractor = Extractor(path=path, url=url, depth=depth, down_ext=down_ext, download_dir=download_dir,
                                       key_words=key_words, encoding=encoding, is_fwf=is_fwf, colnames=colnames, colspecs=colspecs,
                                       sep=sep)
        try:
            self.dataframes = self.extractor.extract()
            logging.info("Extraction completed")

            if delete_data_dir and os.path.exists(download_dir):
                shutil.rmtree(download_dir)
                logging.info(f"Deleted data directory: {download_dir}")

            return self.dataframes
        except Exception as e:
            logging.error(f"Exception while extracting data: {e}")
            raise ValueError(f"Extraction failed: {str(e)}")

    def select_columns(self, dataframe: pd.DataFrame) -> List[str]:
        """
        Allows the user to select columns from the extracted data.

        Args:
            dataframe (pd.DataFrame): DataFrame to select columns from.

        Returns:
            List[str]: A list of selected columns.
        """
        print("----------------------")
        print("Selecting columns...")

        available_columns = dataframe.columns.tolist()

        print("Available columns:")
        print(available_columns)

        selected_columns = input("Enter the columns you want to select, separated by commas (leave blank for all): ")
        if not selected_columns.strip():
            return available_columns
        else:
            return [col.strip() for col in selected_columns.split(',')]

    def transform(self, delete_files=False) -> List[pd.DataFrame]:
        """
        Transforms extracted data into Parquet files.

        Args:
            delete_files (bool): Whether to delete the original files after transformation.

        Returns:
            List[pd.DataFrame]: Transformed data as DataFrames.
        """
        print("----------------------")
        print("Transforming data...")

        if not isinstance(self.dataframes, list) or not self.dataframes:
            print("Error: DataFrame list is either not a list or is empty.")
            raise ValueError("Empty DataFrame list. Check extraction process.")

        for dataframe in tqdm(self.dataframes, desc="Transforming files"):
            if dataframe is not None:
                try:
                    selected_columns = self.select_columns(dataframe)

                    output_file_path = os.path.join("data/output", f"{dataframe.name}.parquet")

                    if self.transformer is None:
                        self.transformer = Transformer(dataframe, output_file_path, columns=selected_columns)

                    detected_dtypes = self.transformer.auto_detect_dtypes()
                    print(f"Detected dtypes: {detected_dtypes}")

                    self.transformer.dTypes = detected_dtypes

                    self.transformer.transform_and_save()

                    if delete_files:
                        os.remove(dataframe.file_path)
                        print(f"Deleted original file: {dataframe.file_path}")

                except Exception as e:
                    print(f"Exception while transforming data: {e}")
                    raise ValueError(f"Transformation failed: {str(e)}")
            else:
                print("Warning: DataFrame is None, skipping...")

        print("Successful transformation")
        return self.dataframes

    def export(self, df: pd.DataFrame, file_name: str):
        """
        Exports a DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to export.
            file_name (str): The name of the file (without extension).
        """
        file_name = file_name.replace(" ", "_")
        output_file = f"data/output/{file_name}.csv"

        try:
            df.to_csv(output_file, index=False)
            print(f"Exported DataFrame to {output_file}")
        except Exception as e:
            print(f"Error exporting DataFrame: {e}")
            raise ValueError(f"Error exporting DataFrame: {str(e)}")