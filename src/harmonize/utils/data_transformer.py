import pandas as pd
from pandas import DataFrame
from abc import abstractmethod
from typing import Any, Callable, Dict, Optional, Tuple, Union


class BaseDataTransformer:
    """Abstract Data Transformer class

    """
    def __new__(cls, operation: str):
        subclass_map = {subclass.operation: subclass for subclass in cls.__subclasses__()}
        subclass = subclass_map[operation]
        instance = super(BaseDataTransformer, subclass).__new__(subclass)
        return instance

    @abstractmethod
    def apply_transform(self, df: DataFrame, args: Dict) -> DataFrame:
        """_summary_

        Args:
            df (DataFrame): Dataframe to transform

        Raises:
            NotImplementedError: _description_

        Returns:
            DataFrame: _description_
        """
        raise NotImplementedError()


class RenameColumns(BaseDataTransformer):
    """Rename Columns Data Transformer class

    Args:
        BaseDataTransformer: Abstract Data transformer class
    """
    operation = 'RenameColumns'

    def apply_transform(self, df: DataFrame, args: Dict) -> DataFrame:
        """Rename columns based on a list of old and new names

        Args:
            df (DataFrame): Dataframe to rename
            args (Dict): Arguments dictionary

        Returns:
            DataFrame: Renamned Dataframe
        """
        old_names = args['old_names']
        new_names = args['new_names']
        if len(old_names) != len(new_names):
            raise ValueError('Old and new names must be of same length')
        mapping_dict = {old_name: new_name for (old_name,
                                                new_name) in zip(old_names,
                                                                 new_names)}
        df.rename(columns=mapping_dict, inplace=True)
        return df


class SelectColumns(BaseDataTransformer):
    """Select Columns Data Transformer class

    Args:
        BaseDataTransformer: Abstract Data transformer class
    """
    operation = 'SelectColumns'

    def apply_transform(self, df: DataFrame, args: Dict) -> DataFrame:
        """Returns a Dataframe with a given set of columns based on a
        list of column names

        Args:
            df (DataFrame): Dataframe to rename
            args (Dict): Arguments dictionary

        Returns:
            DataFrame: Subset of Dataframe
        """
        if 'columns' not in args:
            raise ValueError('Columns argument was not passed')
        columns = args['columns']
        if len(columns) == 0:
            raise ValueError('No columns were given for select.')
        df_select = df.loc[:, columns].copy()
        return df_select


class GroupBy(BaseDataTransformer):
    """Group By Data Transformer class

    Args:
        BaseDataTransformer: Abstract Data transformer class
    """
    operation = 'GroupBy'

    def apply_transform(self, df: DataFrame, args: Dict) -> DataFrame:
        """Computes a new dataframe with some aggregations based on arguments.
        Args:
            df (DataFrame): Input Dataframe
            args (Dict): Arguments dictionary

        Returns:
            DataFrame: Aggregate Dataframe
        """
        if 'group_by' not in args:
            raise ValueError('Group by columns argument was not passed')
        group_by_columns = args['group_by']
        if len(group_by_columns) == 0:
            raise ValueError('No columns were given for group by.')
        if 'aggregation' not in args:
            raise ValueError('Aggregation argument was not passed')
        aggregate_params = args['aggregation']
        dfs = []
        for column, operation in aggregate_params.items():
            if operation == 'MAX':
                df_aggregate = df.groupby(group_by_columns)[column].max()
            elif operation == 'MIN':
                df_aggregate = df.groupby(group_by_columns)[column].min()
            elif operation == 'MEAN':
                df_aggregate = df.groupby(group_by_columns)[column].mean()
            elif operation == 'COUNT':
                df_aggregate = df.groupby(group_by_columns)[column].count()
            else:
                df_aggregate = df.groupby(group_by_columns)[column].count()
            df_aggregate.rename(column, inplace=True)
            dfs.append(df_aggregate)
        df_aggregate = pd.concat(dfs).reset_index()
        return df_aggregate
