from typing import Dict
from pandas import DataFrame
from harmonize.utils.handler import BaseDataHandler
from harmonize.utils.data_transformer import BaseDataTransformer
from harmonize.utils.utils import get_path_extension


class SourceData:
    """Class that handles the data associated with a given source.
    """
    def __init__(self, source_config: Dict = {},
                 data: DataFrame = DataFrame()):
        self._config = source_config
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def source_config(self):
        return self._config

    def load_data(self):
        """Loads the data into a dataframe for a given source using the handler

        Returns:
            Dataframe: Data from the source
        """
        handler = BaseDataHandler(self._config['source_format'])
        self._data = handler.load_data(config=self._config)
        if 'transformations' in self._config:
            self._data = self.apply_transformations()
        return self._data

    def apply_transformations(self):
        """Apply transformations to data defined in configuration

        Returns:
            Dataframe: Transformed Data from the source
        """
        df_transform = self._data.copy()
        if 'transformations' in self._config:
            transformations = self._config['transformations']
            for transformation in transformations:
                for operation, args in transformation.items():
                    ops = BaseDataTransformer(operation)
                    df_transform = ops.apply_transform(df_transform, args)
        return df_transform

    def save_data(self, path: str):
        """Save the data in a given path

        Args:
            path (str): Local path to save data
        """
        file_format = get_path_extension(path)
        handler = BaseDataHandler(file_format)
        handler.save_data(self._data, path)
