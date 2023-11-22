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

from pandas import DataFrame

from nyctibius.dto.data_info import DataInfo
from nyctibius.enums.config_enum import ConfigEnum
from nyctibius.etl.loader import Loader
from nyctibius.etl.transformer import Transformer
from etl.extractor import Extractor


class Harmonizer():

    def __init__(self, datasets: List[DataInfo]):
        self._datasets = datasets

    def extract(self, urls: List[str]):
        extractor = Extractor()
        extractor.run_scrapy_spider()
        extractor.extract()
        # TODO create a DataInfo obj according to extractor location
        return self

    def transform(self, table_name, headers=None) -> List[DataInfo]:
        for dataset in self._datasets:
            if dataset is not None:
                transformer = Transformer(dataset.file_path, ConfigEnum.DB_PATH.value, table_name, headers)
                transformed_data = transformer.transform_data()
                dataset.data(transformed_data)
        return self._datasets

    def load(self) -> List[tuple]:
        results = []
        for dataset in self._datasets:
            if dataset is not None:
                try:
                    loader = Loader()
                    loader.load_data(dataset)
                    results.append((True, "Data loaded successfully"))
                except Exception as e:
                    results.append((False, f"Error loading data: {str(e)}"))
            else:
                results.append((False, "No dataset to load"))
        return results
