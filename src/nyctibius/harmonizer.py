"""Harmonize main module

This module has function that reads and transform data based on configuration
files for different types of sources.

This script requires that `pandas` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * list_sources - returns the current list of sources
"""
import time
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

    def extract(self, path=None, url=None, depth=0, ext = ['.csv','.xls','.xlsx','.zip']):
        print("----------------------")
        print("Extracting ...")
        extractor = Extractor(path, url, depth, down_ext=ext)
        list_datainfo = extractor.extract()
        self._dataInfoList = list(list_datainfo.values())
        print("Extraction completed")
        return self._dataInfoList

    def transform(self) -> List[DataInfo]:
        print("----------------------")
        print("Transforming ...")
        for dataset in tqdm(self._dataInfoList):
            if dataset is not None:
                transformer = Transformer(dataset.file_path, ConfigEnum.DB_PATH.value)
                transformed_data = transformer.transform_data(dataset.name)
                dataset.data = transformed_data
        print("Successful transformation")
        return self._dataInfoList

    def load(self) -> List[tuple]:
        results = []
        loader = Loader()
        print("----------------------")
        print("Loading ...")
        for dataset in tqdm(self._dataInfoList):
            if dataset is not None:
                try:
                    loader.load_data(dataset)
                    #results.append((True, "Data loaded successfully"))
                except Exception as e:
                    results.append((False, f"Error loading data: {str(e)}"))
            else:
                results.append((False, "No dataset to load"))
        print("Data loaded successfully")
        return results
