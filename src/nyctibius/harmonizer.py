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

from dto.data_info import DataInfo
from enums.config_enum import ConfigEnum
from etl.loader import Loader
from etl.transformer import Transformer
from etl.extractor import Extractor


class Harmonizer:

    def __init__(self, dataInfoList: List[DataInfo] = None):
        self._dataInfoList = dataInfoList if dataInfoList is not None else []

    def extract(self, url=None, depth=0):
        extractor = Extractor(url, depth)
        extractor.run_standard_spider()
        list_datainfo = extractor.extract()
        self._dataInfoList = list(list_datainfo.values())
        return self._dataInfoList

    def transform(self) -> List[DataInfo]:
        for dataset in self._dataInfoList:
            if dataset is not None:
                transformer = Transformer(dataset.file_path, ConfigEnum.DB_PATH.value)
                transformed_data = transformer.transform_data(dataset.name)
                dataset.data = transformed_data
        return self._dataInfoList

    def load(self) -> List[tuple]:
        results = []
        loader = Loader()
        for dataset in self._dataInfoList:
            if dataset is not None:
                try:
                    start_time = time.time()
                    loader.load_data(dataset)
                    print("Tiempo: ", time.time() - start_time)
                    results.append((True, "Data loaded successfully"))
                except Exception as e:
                    results.append((False, f"Error loading data: {str(e)}"))
            else:
                results.append((False, "No dataset to load"))
        return results
