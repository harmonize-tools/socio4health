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
import logging


class Harmonizer:

    def __init__(self, dataInfoList: List[DataInfo] = None):
        self._dataInfoList = dataInfoList if dataInfoList is not None else []
    def extract(self, path, url, depth, ext):
        print("----------------------")
        print("Extracting ...")
        try:
            extractor = Extractor(path, url, depth, down_ext=ext, download_dir="data/input")
            list_datainfo = extractor.extract()
            self._dataInfoList = list(list_datainfo.values())
            print("Extraction completed")
            return self._dataInfoList
        except:
            logging.error(f"Empty DataInfo")
            self._dataInfoList = list_datainfo
            return self._dataInfoList

    def transform(self) -> List[DataInfo]:
        print("----------------------")
        print("Transforming ...")
        if isinstance(self._dataInfoList, list) and self._dataInfoList:
            for dataset in tqdm(self._dataInfoList):
                if dataset is not None:
                    transformer = Transformer(dataset.file_path, ConfigEnum.DB_PATH.value)
                    transformed_data = transformer.transform_data(dataset.name)
                    dataset.data = transformed_data
            print("Successful transformation")
        else:
            raise ValueError(f"Empty DataInfo. Check extraction process\n{self._dataInfoList}")
        return self._dataInfoList

    def load(self) -> List[tuple]:
        results = []
        loader = Loader()
        print("----------------------")
        print("Loading ...")
        if isinstance(self._dataInfoList, list) and self._dataInfoList:
            for dataset in tqdm(self._dataInfoList):
                if dataset is not None:
                    try:
                        loader.load_data(dataset)
                    except Exception as e:
                        results.append((False, f"Error loading data: {str(e)}"))
                else:
                    results.append((False, "No dataset to load"))
            print("Data loaded successfully")
        else:
            raise ValueError(f"Empty DataInfo. Check extraction process")
        return results
