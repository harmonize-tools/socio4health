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

from nyctibius.dto.dataset import Dataset
from nyctibius.enums.config_enum import ConfigEnum
from nyctibius.etl.loader import Loader
from nyctibius.etl.transformer import Transformer


class Harmonizer:

    def __init__(self, datasets: List[Dataset]):
        self._datasets = datasets

    def transform(self, table_name, headers=None) -> List[DataFrame]:
        dataframes = []
        for dataset in self._datasets:
            if dataset is not None:
                transformer = Transformer(dataset.filepath, ConfigEnum.DB_PATH.value, table_name, headers)
                transformed_data = transformer.transform_data()
                dataframes.append(transformed_data)
        # TODO need to set tech dataframe into corresponding data in dataframe
        return dataframes

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
