import os
from typing import List
import pandas as pd
from tqdm import tqdm
from .dto.data_info import DataInfo
from .etl.transformer import Transformer
from .etl.extractor import Extractor

class Harmonizer:

    def __init__(self, dataInfoList: List[DataInfo] = None):
        self._dataInfoList = dataInfoList if dataInfoList is not None else []

    def extract(self, path=None, url=None, depth=0, down_ext=['.csv', '.xls', '.xlsx', ".txt", ".sav", ".zip"],
                download_dir="data/input", key_words=[]) -> List[DataInfo]:
        """
        Extract data based on the provided configuration.

        Args:
            path (str): Local path to extract files from.
            url (str): URL to download data from.
            depth (int): Depth for recursive directory search.
            down_ext (list): List of file extensions to download.
            download_dir (str): Directory to save downloaded files.
            key_words (list): Keywords to filter the files.

        Returns:
            List[DataInfo]: A list of extracted data information.
        """
        print("----------------------")
        print("Extracting data...")
        extractor = Extractor(path=path, url=url, depth=depth, down_ext=down_ext, download_dir=download_dir, key_words=key_words)
        try:
            list_datainfo = extractor.extract()
            self._dataInfoList = list(list_datainfo.values())
            print("Extraction completed")
            return self._dataInfoList
        except Exception as e:
            print(f"Exception while extracting data: {e}")
            raise ValueError(f"Extraction failed: {str(e)}")

    def transform(self, delete_files=False) -> List[DataInfo]:
        """
        Transforms extracted data into Parquet files.

        Args:
            delete_files (bool): Whether to delete the original files after transformation.

        Returns:
            List[DataInfo]: Transformed data information.
        """
        print("----------------------")
        print("Transforming data...")

        if not isinstance(self._dataInfoList, list) or not self._dataInfoList:
            print("Error: DataInfo list is either not a list or is empty.")
            raise ValueError("Empty DataInfo. Check extraction process.")

        for dataset in tqdm(self._dataInfoList, desc="Transforming files"):
            if dataset is not None:
                try:
                    output_file_path = os.path.join("data/output", f"{os.path.splitext(os.path.basename(dataset.file_path))[0]}.parquet")
                    transformer = Transformer(dataset.file_path, output_file_path)
                    transformer.transform_and_save()

                    if delete_files:
                        os.remove(dataset.file_path)
                        print(f"Deleted original file: {dataset.file_path}")

                except Exception as e:
                    print(f"Exception while transforming data: {e}")
                    raise ValueError(f"Transformation failed for {dataset.file_path}: {str(e)}")
            else:
                print("Warning: Dataset is None, skipping...")

        print("Successful transformation")
        return self._dataInfoList

    def export(self, df: pd.DataFrame, file_name: str):
        """
        Exports a DataFrame to a CSV file.

        Args:
            df (DataFrame): The DataFrame to export.
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
