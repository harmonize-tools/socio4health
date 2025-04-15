import os
import shutil
from typing import Optional, Tuple
from typing import List
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
from tqdm import tqdm
import logging
from socio4health.extractor import Extractor
from socio4health.enums.data_info_enum import NameEnum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def vertical_merge(ddfs: List[dd.DataFrame], min_common_columns=1, similarity_threshold=0.8):
    """
    Merge a list of Dask DataFrames vertically.

    Parameters:
    - ddfs: List of Dask DataFrames to merge
    - min_common_columns: Minimum common columns required (default: 1)
    - similarity_threshold: Column similarity threshold (default: 0.8)

    Returns:
    - List of merged Dask DataFrames
    """
    if not ddfs:
        return []

    groups = []
    used_indices = set()

    for i, df1 in enumerate(tqdm(ddfs, desc="Grouping DataFrames")):
        if i in used_indices:
            continue

        cols1 = set(df1.columns)
        dtypes1 = {col: str(df1[col].dtype) for col in df1.columns}
        current_group = [i]
        used_indices.add(i)

        for j, df2 in enumerate(ddfs[i + 1:]):
            j_actual = i + 1 + j  # Adjust index for the full list
            if j_actual in used_indices:
                continue

            cols2 = set(df2.columns)
            common_cols = cols1 & cols2
            similarity = len(common_cols) / max(len(cols1), len(cols2))

            if (len(common_cols) >= min_common_columns and
                    similarity >= similarity_threshold):

                compatible = True
                for col in common_cols:
                    if col in dtypes1 and col in df2.columns:
                        if str(df2[col].dtype) != dtypes1[col]:
                            compatible = False
                            break

                if compatible:
                    current_group.append(j_actual)
                    used_indices.add(j_actual)
                    cols1.update(cols2)
                    for col in cols2 - cols1:
                        dtypes1[col] = str(df2[col].dtype)

        groups.append(current_group)

    merged_dfs = []
    for group_indices in tqdm(groups, desc="Merging groups"):
        if len(group_indices) == 1:
            merged_dfs.append(ddfs[group_indices[0]])
        else:
            group_dfs = [ddfs[i] for i in group_indices]
            common_cols = set(group_dfs[0].columns)
            for df in group_dfs[1:]:
                common_cols.intersection_update(df.columns)

            aligned_dfs = []
            for df in group_dfs:
                common_cols_ordered = [col for col in df.columns if col in common_cols]
                other_cols = [col for col in df.columns if col not in common_cols]
                aligned_dfs.append(df[common_cols_ordered + other_cols])

            merged_df = dd.concat(aligned_dfs, axis=0, ignore_index=True)
            merged_dfs.append(merged_df)

    return merged_dfs


def drop_nan_columns(ddf, threshold=1.0, sample_frac=None):
    """
    Drop columns where the majority of values are NaN.

    Parameters:
    -----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame
    threshold : float, optional (default=0.5)
        Drop columns with NaN percentage above this threshold (0.0 to 1.0)
    sample_frac : float or None, optional (default=None)
        If specified, uses sampling to estimate NaN percentages (faster for large DataFrames)

    Returns:
    --------
    dask.dataframe.DataFrame
        DataFrame with columns dropped
    """
    logging.info("Dropping columns with majority NaN values...")

    if not 0 <= threshold <= 1:
        raise ValueError("Threshold must be between 0 and 1")

    if sample_frac is not None:
        if not 0 < sample_frac <= 1:
            raise ValueError("sample_frac must be between 0 and 1")
        sample = ddf.sample(frac=sample_frac).compute()
        nan_percentages = sample.isna().mean()
    else:
        nan_percentages = ddf.isna().mean().compute()

    columns_to_drop = nan_percentages[nan_percentages > threshold].index.tolist()

    if columns_to_drop:
        logging.info(f"Dropping columns with >{threshold * 100:.0f}% NaN values: {columns_to_drop}")
        return ddf.drop(columns=columns_to_drop)
    else:
        logging.info("No columns with majority NaN values found")
        return ddf


class Harmonizer:

    def __init__(self,
                 extractor: Optional[Extractor] = None,
                 input_folder: str = "data/input",
                 name: str = None,
                 url: Optional[str] = None,
                 country: Optional[str] = None,
                 year: Optional[int] = None,
                 selected_columns: Optional[List[str]] = None):
        """
        Initialize the Harmonizer with a list of DataFrames.
        Args:
            extractor (Extractor): Extractor instance.
            input_folder (str): Input folder path.
            name (str): Name of the dataset.
            url (str): URL of the dataset.
            country (str): Country of the dataset.
            year (int): Year of the dataset.
            selected_columns (list): List of selected columns.
        """
        self.extractor = extractor
        self.input_folder = input_folder
        self.name = name
        self.url = url
        self.country = country
        self.year = year
        self.selected_columns = selected_columns

    @property
    def extractor(self) -> Extractor:
        """Get the Extractor instance."""
        return self._extractor
    @extractor.setter
    def extractor(self, value: Extractor):
        if not isinstance(value, (Extractor, type(None))):
            raise TypeError("extractor must be an Extractor instance or None")
        self._extractor = value

    @property
    def input_folder(self) -> str:
        """Get the input folder path."""
        return self._input_folder
    @input_folder.setter
    def input_folder(self, value: str):
        if not isinstance(value, str):
            raise TypeError("input_folder must be a string")
        self._input_folder = value

    @property
    def name(self) -> Optional[str]:
        """Get the dataset name."""
        return self._name
    @name.setter
    def name(self, value: str):
        if not isinstance(value, (str, type(None))):
            raise TypeError("name must be a string or None")
        self._name = value

    @property
    def url(self) -> str:
        """Get the dataset URL."""
        return self._url
    @url.setter
    def url(self, value: str):
        if not isinstance(value, (str, type(None))):
            raise TypeError("url must be a string or None")
        self._url = value

    @property
    def country(self) -> str:
        """Get the country of the dataset."""
        return self._country
    @country.setter
    def country(self, value: str):
        if not isinstance(value, (str, type(None))):
            raise TypeError("country must be a string or None")
        self._country = value

    @property
    def year(self) -> int:
        """Get the year of the dataset."""
        return self._year
    @year.setter
    def year(self, value: int):
        if not isinstance(value, (int, type(None))):
            raise TypeError("year must be an integer or None")
        self._year = value

    @property
    def selected_columns(self) -> Optional[List[str]]:
        """Get the selected columns."""
        return self._selected_columns
    @selected_columns.setter
    def selected_columns(self, value: List[str]):
        if not isinstance(value, (list, type(None))):
            raise TypeError("selected_columns must be a list or None")
        self._selected_columns = value