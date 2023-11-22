"""Base Data Handler interface

This intergace has function to read and write  different types of sources.

This script requires that `pandas` be installed within the Python
environment you are running this script in.
"""
from abc import abstractmethod
from typing import Dict
from pandas import DataFrame
import pandas as pd
import requests
from harmonize import utils


class BaseDataHandler:
    """Abstract Data Handler class
    """

    def __new__(cls, format: str):
        subclass_map = {subclass.format: subclass for subclass in cls.__subclasses__()}
        subclass = subclass_map[format]
        instance = super(BaseDataHandler, subclass).__new__(subclass)
        return instance

    @abstractmethod
    def load_data(self,
                  config: Dict,
                  **kwargs
                  ) -> DataFrame:
        """Abstract read method for a given data type

        Args:
            config (Dict): Configuration dictionary to extract data

        Returns:
            DataFrame: Data loaded into a Pandas dataframe
        """
        raise NotImplementedError()

    @abstractmethod
    def save_data(self,
                  dataframe: DataFrame,
                  path: str,
                  **kwargs) -> None:
        """Abstract write method for a given data type

        Args:
            dataframes (DataFrame): Dataframe with data
            path (str): Location to write data to
        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError()


class CsvDataHandler(BaseDataHandler):
    """Csv Data Handler class

    Args:
        BaseDataHandler (BaseDataHandler): Abstract Data handler class
    """
    format = 'CSV'

    def load_data(self,
                  config: Dict,
                  **kwargs
                  ) -> DataFrame:
        """Read an csv file based on extraction configuration

        Args:
            path (str): Path to the csv file
            extraction_conf (Union[Dict, List[Dict]]): Configuration to extract
                data.

        Returns:
            Union[DataFrame, List[DataFrame]]: List of dataframes with data
        """
        extraction_conf = config['extraction_params']
        path = extraction_conf['url']
        if 'pandas_args' in extraction_conf:
            pandas_args = extraction_conf['pandas_args']
            dataframe = pd.read_csv(path, **pandas_args, **kwargs)
        else:
            dataframe = pd.read_csv(path, **kwargs)
        return dataframe

    def save_data(self,
                  dataframe: DataFrame,
                  path: str,
                  **kwargs):
        """Saves a dataframe as a csv file

        Args:
            dataframes (Union[DataFrame, List[DataFrame]]): List of dataframes
            path (str): Path to save the excel file
            kwargs (Dict): dictionary of arguments
        """
        dataframe.to_csv(path,index=False, **kwargs)


class XlsxDataHandler(BaseDataHandler):
    """Xlsx Data Handler class

    Args:
        BaseDataHandler (BaseDataHandler): Abstract Data Handler class
    """
    format = 'XLSX'

    def load_data(self,
                  config: Dict,
                  **kwargs
                  ) -> DataFrame:
        """Read an xlsx file based on a given configuration

        Args:
            config (Union[Dict, List[Dict]]): Configuration to extract
                data.

        Returns:
            Union[DataFrame, List[DataFrame]]: List of dataframes with data
        """
        extraction_conf = config['extraction_params']
        path = extraction_conf['url']
        if utils.is_url(path):
            data = requests.get(path, timeout=60).content
        else:
            data = path
        xls_file = pd.ExcelFile(data)
        if 'pandas_args' in extraction_conf:
            pandas_args = extraction_conf['pandas_args']
            dataframe = xls_file.parse(**pandas_args, **kwargs)
        else:
            dataframe = xls_file.parse(**kwargs)
        if extraction_conf['merge_cells']:
            dataframe = dataframe.fillna(method='ffill', axis=0)
        return dataframe

    def save_data(self,
                  dataframe: DataFrame,
                  path: str,
                  **kwargs):
        """Saves a dataframe as a xlsx file

        Args:
            dataframes (Union[DataFrame, List[DataFrame]]): List of dataframes
            path (str): Path to save the excel file
            kwargs (Dict): dictionary of arguments
        """
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        dataframe.to_excel(writer, index=False, **kwargs)
        writer.close()