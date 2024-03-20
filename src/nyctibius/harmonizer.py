"""Harmonize main module

This module has function that reads and transform data based on configuration
files for different types of sources.

This script requires that `pandas` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * list_sources - returns the current list of sources
"""
from typing import List
from .dto.data_info import DataInfo
from .enums.config_enum import ConfigEnum
from .etl.loader import Loader
from .etl.transformer import Transformer
from .etl.extractor import Extractor
from tqdm import tqdm


class Harmonizer:

    def __init__(self, dataInfoList: List[DataInfo] = None):
        self._dataInfoList = dataInfoList if dataInfoList is not None else []

    def extract(self, path=None, url=None, depth=0, down_ext=['.csv','.xls','.xlsx', ".txt", ".sav", ".zip"], download_dir="data/input") -> list:
        print("----------------------")
        print("Extracting ...")
        extractor = Extractor(path=path, url=url, depth=depth, down_ext=down_ext, download_dir=download_dir)
        list_datainfo = extractor.extract()
        try:
            list(list_datainfo.values())[0]
            self._dataInfoList = list(list_datainfo.values())
            print("Extraction completed")
            return self._dataInfoList
        except Exception as e:
            print(f"Exception while extracting data: {e}")


    def transform(self) -> List[DataInfo]:
        print("----------------------")
        print("Transforming ...")
        if isinstance(self._dataInfoList, list) and self._dataInfoList:
            for dataset in tqdm(self._dataInfoList):
                if dataset is not None:
                    try:
                        transformer = Transformer(dataset.file_path, ConfigEnum.DB_PATH.value)
                        transformed_data = transformer.transform_data()
                        dataset.data = transformed_data
                    except Exception as e:
                        print(f"\nException while transforming data: {e}")
                else:
                    print("Dataset is None")
            print("Successful transformation")
        else:
            print("self._dataInfoList is not a list or is empty")
            raise ValueError(f"Empty DataInfo. Check extraction process")
        return self._dataInfoList

    def load(self):
        loader = Loader()
        print("----------------------")
        print("Loading ...")
        if isinstance(self._dataInfoList, list) and self._dataInfoList:
            for dataset in tqdm(self._dataInfoList):
                if dataset is not None:
                    try:
                        loader.load_data(dataset)
                    except Exception as e:
                        print(f"\nException while loading data: {e}")
                else:
                    print("Dataset is None")
            print("Data loaded successfully")
        else:
            print("self._dataInfoList is not a list or is empty")
            raise ValueError(f"Empty DataInfo. Check extraction process")
