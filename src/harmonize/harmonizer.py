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

from harmonize.dto.dataset import Dataset
from harmonize.enum.config_enum import ConfigEnum
from harmonize.etl.loader import Loader
from harmonize.etl.transformer import Transformer


class Harmonizer:

    def __init__(self, datasets: List[Dataset]):
        self._datasets = datasets

    def transform(self, table_name, in_col_names, out_col_names) -> List[DataFrame]:
        transformed_datasets = []
        for dataset in self._datasets:
            if dataset is not None:
                transformer = Transformer(dataset.name, in_col_names, out_col_names, ConfigEnum.DB_PATH.value,
                                          table_name)
                transformed_data = transformer.transform_data()
                transformed_datasets.append(transformed_data)
        return transformed_datasets

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
