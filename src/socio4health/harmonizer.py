"""
Harmonizer class for harmonizing and processing Dask DataFrames in health data integration.

"""

import json
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Union, Type, List
import dask.dataframe as dd
import pandas as pd
from tqdm import tqdm
import logging

from socio4health.enums.dict_enum import ColumnMappingEnum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Harmonizer:
    """
        Initialize the Harmonizer class for harmonizing and processing `Dask <https://docs.dask.org>`_ DataFrames in health data integration.

        Attributes
        ----------
        min_common_columns : int
            Minimum number of common columns required for vertical merge (default is 1).
        similarity_threshold : float
            Similarity threshold to consider for vertical merge (default is 0.8).
        nan_threshold : float
            Percentage threshold of ``NaN`` values to drop columns (default is 1.0).
        sample_frac : float or ``None``
            Fraction of rows to sample for NaN detection (default is ``None``).
        column_mapping : ``Enum``, dict, str or ``Path``
            Column mapping configuration (default is ``None``).
        value_mappings : ``Enum``, dict, str or ``Path``
            Categorical value mapping configuration (default is ``None``).
        theme_info : dict, str or ``Path``
             Theme/category information (default is ``None``).
        default_country : str
            Default country for mapping (default is ``None``).
        strict_mapping : bool
            Whether to enforce strict mapping of columns and values (default is ``False``).
        dict_df : pandas.DataFrame
            DataFrame with variable dictionary (default is ``None``).
        categories : list of str
            Categories for data selection (default is an empty list).
        key_col : str
            Key column for data selection (default is ``None``).
        key_val : list of str, int or float
            Key values for data selection (default is an empty list).
        extra_cols : list of str
            Extra columns for data selection (default is an empty list).
        join_key : str
            Key column for joining DataFrames (default is ``None``).
        aux_key : str
            Auxiliary key column for joining DataFrames (default is ``None``).
    """
    def __init__(self,
                 min_common_columns: int = 1,
                 similarity_threshold: float = 1,
                 nan_threshold: float = 1.0,
                 sample_frac: Optional[float] = None,
                 column_mapping: Optional[Union[Type[Enum], Dict[str, Dict[str, str]], str, Path]] = None,
                 value_mappings: Optional[Union[Type[Enum], Dict[str, Dict[str, Dict[str, str]]], str, Path]] = None,
                 theme_info: Optional[Union[Dict[str, List[str]], str, Path]] = None,
                 default_country: Optional[str] = None,
                 strict_mapping: bool = False,
                 dict_df: Optional[pd.DataFrame] = None,
                 categories: Optional[List[str]] = None,
                 key_col: Optional[str] = None,
                 key_val: Optional[List[Union[str, int, float]]] = None,
                 extra_cols: Optional[List[str]] = None,
                 join_key: str = None,
                 aux_key: Optional[str] = None):
        """
        Initialize the Harmonizer class with default parameters.
        """
        self.min_common_columns = min_common_columns
        self.similarity_threshold = similarity_threshold
        self.nan_threshold = nan_threshold
        self.sample_frac = sample_frac
        self.column_mapping = column_mapping
        self.value_mappings = value_mappings
        self.theme_info = theme_info
        self.default_country = default_country
        self.strict_mapping = strict_mapping
        self.dict_df = dict_df
        self.categories = categories or []
        self.key_col = key_col
        self.key_val = key_val or []
        self.extra_cols = extra_cols or []
        self.join_key = join_key
        self.aux_key = aux_key



    # Getters
    @property
    def min_common_columns(self) -> int:
        """Get the minimum number of common columns required for vertical merge."""
        return self._min_common_columns

    @property
    def similarity_threshold(self) -> float:
        """Get the similarity threshold for vertical merge."""
        return self._similarity_threshold

    @property
    def nan_threshold(self) -> float:
        """Get the NaN threshold for column dropping."""
        return self._nan_threshold

    @property
    def sample_frac(self) -> Optional[float]:
        """Get the sampling fraction for NaN detection."""
        return self._sample_frac

    @property
    def column_mapping(self) -> Optional[Union[Type[Enum], Dict[str, Dict[str, str]], str, Path]]:
        """Get the column mapping configuration."""
        return self._column_mapping

    @property
    def value_mappings(self) -> Optional[Union[Type[Enum], Dict[str, Dict[str, Dict[str, str]]], str, Path]]:
        """Get the value mappings configuration."""
        return self._value_mappings

    @property
    def theme_info(self) -> Optional[Union[Dict[str, List[str]], str, Path]]:
        """Get the theme information."""
        return self._theme_info

    @property
    def default_country(self) -> Optional[str]:
        """Get the default country."""
        return self._default_country

    @property
    def strict_mapping(self) -> bool:
        """Get whether strict mapping is enabled."""
        return self._strict_mapping

    @property
    def dict_df(self) -> Optional[pd.DataFrame]:
        """Get the dictionary DataFrame."""
        return self._dict_df

    @property
    def categories(self) -> List[str]:
        """Get the categories for data selection."""
        return self._categories

    @property
    def key_col(self) -> Optional[str]:
        """Get the key column for data selection."""
        return self._key_col

    @property
    def key_val(self) -> List[Union[str, int, float]]:
        """Get the key values for data selection."""
        return self._key_val

    @property
    def extra_cols(self) -> List[str]:
        """Get the extra columns for data selection."""
        return self._extra_cols

    # Setters
    @min_common_columns.setter
    def min_common_columns(self, value: int):
        """Set the minimum number of common columns required for vertical merge."""
        if not isinstance(value, int) or value < 0:
            raise ValueError("min_common_columns must be a non-negative integer")
        self._min_common_columns = value

    @similarity_threshold.setter
    def similarity_threshold(self, value: float):
        """Set the similarity threshold for vertical merge."""
        if not isinstance(value, (int, float)) or not 0 <= value <= 1:
            raise ValueError("similarity_threshold must be a float between 0 and 1")
        self._similarity_threshold = float(value)

    @nan_threshold.setter
    def nan_threshold(self, value: float):
        """Set the NaN threshold for column dropping."""
        if not isinstance(value, (int, float)) or not 0 <= value <= 1:
            raise ValueError("nan_threshold must be a float between 0 and 1")
        self._nan_threshold = float(value)

    @sample_frac.setter
    def sample_frac(self, value: Optional[float]):
        """Set the sampling fraction for NaN detection."""
        if value is not None and (not isinstance(value, (int, float)) or not 0 < value <= 1):
            raise ValueError("sample_frac must be None or a float between 0 and 1")
        self._sample_frac = value

    @column_mapping.setter
    def column_mapping(self, value: Optional[Union[Type[Enum], Dict[str, Dict[str, str]], str, Path]]):
        """Set the column mapping configuration."""
        self._column_mapping = value

    @value_mappings.setter
    def value_mappings(self, value: Optional[Union[Type[Enum], Dict[str, Dict[str, Dict[str, str]]], str, Path]]):
        """Set the value mappings configuration."""
        self._value_mappings = value

    @theme_info.setter
    def theme_info(self, value: Optional[Union[Dict[str, List[str]], str, Path]]):
        """Set the theme information."""
        self._theme_info = value

    @default_country.setter
    def default_country(self, value: Optional[str]):
        """Set the default country."""
        self._default_country = value

    @strict_mapping.setter
    def strict_mapping(self, value: bool):
        """Set whether strict mapping is enabled."""
        if not isinstance(value, bool):
            raise ValueError("strict_mapping must be a boolean")
        self._strict_mapping = value

    @dict_df.setter
    def dict_df(self, value: Optional[pd.DataFrame]):
        """Set the dictionary DataFrame."""
        if value is not None and not isinstance(value, pd.DataFrame):
            raise ValueError("dict_df must be a pandas DataFrame or None")
        self._dict_df = value

    @categories.setter
    def categories(self, value: List[str]):
        """Set the categories for data selection."""
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError("categories must be a list of strings")
        self._categories = value

    @key_col.setter
    def key_col(self, value: Optional[str]):
        """Set the key column for data selection."""
        if value is not None and not isinstance(value, str):
            raise ValueError("key_col must be a string or None")
        self._key_col = value

    @key_val.setter
    def key_val(self, value: List[Union[str, int, float]]):
        """Set the key values for data selection."""
        if not isinstance(value, list):
            raise ValueError("key_val must be a list")
        self._key_val = value

    @extra_cols.setter
    def extra_cols(self, value: List[str]):
        """Set the extra columns for data selection."""
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError("extra_cols must be a list of strings")
        self._extra_cols = value

    def s4h_vertical_merge(self, ddfs: List[dd.DataFrame]) -> List[dd.DataFrame]:
        """
        Merge a list of `Dask <https://docs.dask.org>`_ DataFrames vertically using instance parameters.

        Parameters
        ----------
        ddfs : list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            List of `Dask <https://docs.dask.org>`_ DataFrames to be merged.

        Returns
        -------
        list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            List of merged `Dask <https://docs.dask.org>`_ DataFrames, where each group contains DataFrames with sufficient column overlap and compatible data types.

        Notes
        -----
        - DataFrames are grouped and merged if they share at least ``min_common_columns`` columns and their column similarity is above ``similarity_threshold``.
        - Only columns with matching data types are considered compatible for merging.
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
                j_actual = i + 1 + j
                if j_actual in used_indices:
                    continue

                cols2 = set(df2.columns)
                common_cols = cols1 & cols2
                similarity = len(common_cols) / max(len(cols1), len(cols2))

                if (len(common_cols) >= self.min_common_columns and
                        similarity >= self.similarity_threshold):

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
        if len(merged_dfs) > 1:
            logging.warning("Dataframes that do not have the same columns could not be merged.")

        return merged_dfs

    def drop_nan_columns(self, ddf_or_ddfs: Union[dd.DataFrame, List[dd.DataFrame]]) -> Union[
        dd.DataFrame, List[dd.DataFrame]]:
        """

        Drop columns where the majority of values are ``NaN`` using instance parameters.


        Parameters
        ----------
        ddf_or_ddfs : `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_ or list of dask.dataframe.DataFrame
            The `Dask <https://docs.dask.org>`_ DataFrame or list of `Dask <https://docs.dask.org>`_ DataFrames to process.

        Returns
        -------
        `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_ or list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            The DataFrame(s) with columns dropped where the proportion of ``NaN`` values is greater than nan_threshold.

        Raises
        ------
        ValueError
            If ``nan_threshold`` is not between 0 and 1, or if ``sample_frac`` is not ``None`` or a float between 0 and 1.
        """
        logging.info("Dropping columns with majority NaN values...")

        if not 0 <= self.nan_threshold <= 1:
            raise ValueError("Threshold must be between 0 and 1")

        def process_ddf(ddf):
            if self.sample_frac is not None:
                if not 0 < self.sample_frac <= 1:
                    raise ValueError("sample_frac must be between 0 and 1")
                sample = ddf.sample(frac=self.sample_frac).compute()
                nan_percentages = sample.isna().mean()
            else:
                nan_percentages = ddf.isna().mean().compute()

            columns_to_drop = nan_percentages[nan_percentages > self.nan_threshold].index.tolist()

            if columns_to_drop:
                logging.info(f"Dropping columns with >{self.nan_threshold * 100:.0f}% NaN values: {columns_to_drop}")
                return ddf.drop(columns=columns_to_drop)
            else:
                logging.info("No columns with majority NaN values found")
                return ddf

        if isinstance(ddf_or_ddfs, list):
            return [process_ddf(ddf) for ddf in ddf_or_ddfs]
        else:
            return process_ddf(ddf_or_ddfs)

    @staticmethod
    def s4h_get_available_columns(df_or_dfs: Union[dd.DataFrame, pd.DataFrame, List[Union[dd.DataFrame, pd.DataFrame]]]) -> \
    List[str]:
        """
        Get a list of unique column names from a single DataFrame or a list of DataFrames.
        Supports both Dask and pandas DataFrames.

        Parameters
        ----------
        df_or_dfs : Union[dask.dataframe.DataFrame, pandas.DataFrame,
                         List[Union[dask.dataframe.DataFrame, pandas.DataFrame]]]
            A single DataFrame or a list of DataFrames to extract column names from.
            Can be Dask DataFrames, pandas DataFrames, or a mix of both.

        Returns
        -------
        list of str
            Sorted list of unique column names across all provided DataFrames.

        Raises
        ------
        TypeError
            If the input is not a DataFrame or a list of DataFrames.
        """
        if isinstance(df_or_dfs, (dd.DataFrame, pd.DataFrame)):
            df_or_dfs = [df_or_dfs]
        elif not isinstance(df_or_dfs, list):
            raise TypeError("Input must be a DataFrame or a list of DataFrames")

        unique_columns = set()
        for df in df_or_dfs:
            if not isinstance(df, (dd.DataFrame, pd.DataFrame)):
                raise TypeError("All elements in the list must be DataFrames (Dask or pandas)")
            unique_columns.update(df.columns)

        return sorted(unique_columns)

    def s4h_harmonize_dataframes(
            self,
            country_dfs: Dict[str, List[dd.DataFrame]]
    ) -> Dict[str, List[dd.DataFrame]]:
        """
        Harmonize `Dask <https://docs.dask.org>`_ DataFrames using the instance parameters.

        Parameters
        ----------
        country_dfs : dict of str to list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            Dictionary mapping country names to lists of `Dask <https://docs.dask.org>`_ DataFrames to be harmonized.

        Returns
        -------
        dict of str to list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            Dictionary mapping country names to lists of harmonized `Dask <https://docs.dask.org>`_ DataFrames.

        Note
        -----
        - Column and value mappings are applied per country using the provided configuration.
        - If ``strict_mapping`` is enabled, unmapped columns or values will raise a ValueError.
        - Column renaming and categorical value harmonization are performed in-place.
        """

        def load_mapping(mapping_input):
            """Helper to load mappings from different input types"""
            if isinstance(mapping_input, (str, Path)):
                if Path(mapping_input).exists():
                    with open(mapping_input) as f:
                        return json.load(f)
                try:
                    return json.loads(mapping_input)
                except json.JSONDecodeError:
                    raise ValueError("Invalid JSON string or file path")
            return mapping_input

        def get_country_mapping(mapping_obj, country):
            """Get mapping for a country from either Enum or dict"""
            if isinstance(mapping_obj, type) and issubclass(mapping_obj, Enum):
                try:
                    return mapping_obj[country].value
                except KeyError:
                    if self.default_country:
                        return mapping_obj[self.default_country].value
                    return {}
            elif isinstance(mapping_obj, dict):
                return mapping_obj.get(country,
                                       mapping_obj.get(self.default_country, {}))
            return {}

        # Load mappings if they're JSON
        column_mapping = load_mapping(self.column_mapping)
        value_mappings = load_mapping(self.value_mappings)
        theme_info = load_mapping(self.theme_info) if self.theme_info else None

        def process_dataframe(df: dd.DataFrame, country: str) -> dd.DataFrame:
            """Process a single dataframe"""
            # Get mappings for this country
            col_map = get_country_mapping(column_mapping, country)
            val_maps = get_country_mapping(value_mappings, country)

            # Validate mappings if in strict mode
            if self.strict_mapping:
                missing_cols = [c for c in df.columns if c not in col_map]
                if missing_cols:
                    raise ValueError(f"Unmapped columns in {country}: {missing_cols}")

            # 1. Harmonize column names
            df = df.rename(columns=col_map)

            # 2. Harmonize categorical values
            for col, val_map in val_maps.items():
                if col in df.columns:
                    # Convert to string first to handle mixed types
                    df[col] = df[col].astype('str')

                    # Map values with validation in strict mode
                    if self.strict_mapping:
                        unique_vals = df[col].drop_duplicates().compute()
                        unmapped = set(unique_vals) - set(val_map.keys())
                        if unmapped:
                            raise ValueError(
                                f"Unmapped values in {country}.{col}: {unmapped}"
                            )

                    df[col] = df[col].map(val_map).astype('category')

            return df

        return {
            country: [process_dataframe(df, country) for df in dfs]
            for country, dfs in country_dfs.items()
        }
    
    def s4h_compare_with_dict(self, ddfs: List[dd.DataFrame]) -> pd.DataFrame:
        """
        Compare the columns available in the DataFrames with the variables in the dictionary and
        return a DataFrame with the columns that do not match in both directions.

        Parameters
        ----------
        ddfs : list of dask.dataframe.DataFrame
            List of DataFrames to evaluate.

        Returns
        -------
        pd.DataFrame
            DataFrame with mismatched columns.
        """
        if self.dict_df is None:
            raise ValueError("dict_df has not been defined in the Harmonizer instance.")

        available_cols = set(self.s4h_get_available_columns(ddfs))

        expected_vars = set(self.dict_df['variable_name'].str.upper().unique())

        in_df_not_in_dict = sorted(available_cols - expected_vars)
        in_dict_not_in_df = sorted(expected_vars - available_cols)

        max_len = max(len(in_df_not_in_dict), len(in_dict_not_in_df))
        in_df_not_in_dict += [None] * (max_len - len(in_df_not_in_dict))
        in_dict_not_in_df += [None] * (max_len - len(in_dict_not_in_df))

        diff_df = pd.DataFrame({
            "Unmatched ddfs variable": in_df_not_in_dict,
            "Unmatched dict_df variables": in_dict_not_in_df
        })

        total = len(available_cols)
        match_count = len(available_cols & expected_vars)
        match_pct = (match_count / total) * 100 if total > 0 else 0

        print(f"Matches with dict_df: {match_count} ({match_pct:.2f}%)")

        return diff_df


    def s4h_data_selector(self, ddfs: List[dd.DataFrame]) -> List[dd.DataFrame]:
        """
        Select rows from `Dask <https://docs.dask.org>`_ DataFrames based on the instance parameters.

        Parameters
        ----------
        ddfs : list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            List of Dask DataFrames to filter.

        Returns
        -------
        list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            List of filtered Dask DataFrames according to the ``key_col``, ``key_val``, ``categories``, and ``extra_cols``.

        Raises
        ------
        KeyError
            If the ``key_col`` is not found in a DataFrame.
        """

        dict_df = self.dict_df.copy()
        dict_df['variable_name'] = dict_df['variable_name'].str.upper()

        key_column_upper = self.key_col.upper() if self.key_col else None

        filtered_ddfs = []
        for ddf in ddfs:
            ddf.columns = ddf.columns.str.upper()

            if self.key_col and self.key_val:
                if key_column_upper not in ddf.columns:
                    raise KeyError(f"Key column '{self.key_col}' not found in DataFrame")

                filtered_ddf = ddf[ddf[key_column_upper].isin([val.upper() if isinstance(val, str) else val
                                                               for val in self.key_val])]
            else:
                logging.warning("key_col or key_val not defined, row-wise size will not be reduced")
                filtered_ddf = ddf

            if isinstance(filtered_ddf, dd.DataFrame):
                n_rows = filtered_ddf.shape[0].compute()
            else:
                n_rows = filtered_ddf.shape[0]

            if n_rows == 0:
                logging.warning("No rows found matching key values in DataFrame")

            dict_df_filtered = dict_df[dict_df[ColumnMappingEnum.CATEGORY.value].isin(self.categories)]
            columns_list = dict_df_filtered[ColumnMappingEnum.VARIABLE_NAME.value].dropna().unique().tolist()

            if self.extra_cols:
                columns_list.extend(col.upper() for col in self.extra_cols if col.upper() not in columns_list)

            if self.join_key:
                columns_list.extend(col.upper() for col in filtered_ddf.columns if col.upper() == self.join_key)

            if columns_list:
                existing_columns = [col for col in columns_list if col in filtered_ddf.columns]

                final_columns = []
                if key_column_upper:
                    final_columns.append(key_column_upper)
                final_columns.extend(existing_columns)
                final_columns = list(dict.fromkeys(final_columns))

                if len(final_columns) == 1:
                    logging.warning("Only key column found in the filtered DataFrame. Use compare_with_dict() for more information.")
                filtered_ddf = filtered_ddf[final_columns]
            else:
                logging.warning("No columns found matching the specified categories. Use compare_with_dict() for more information.")
                filtered_ddf = filtered_ddf[[key_column_upper]] if key_column_upper else filtered_ddf

            filtered_ddfs.append(filtered_ddf)

        return filtered_ddfs

    def s4h_join_data(self, ddfs: List[dd.DataFrame]) -> pd.DataFrame:
        """
        Join multiple `Dask <https://docs.dask.org>`_ DataFrames on a specified ``key_col``, removing duplicate columns.

        Parameters
        ----------
        ddfs : list of `dask.dataframe.DataFrame <https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.html>`_
            List of Dask DataFrames to join.

        Returns
        -------
        pandas.DataFrame
            Merged DataFrame with duplicate columns removed.
        """
        pandas_dfs = [df.compute() for df in ddfs]

        def identify_primary_df(dfs):
            candidates = []

            for i, df in enumerate(dfs):
                if self.join_key not in df.columns:
                    continue

                total_rows = len(df)
                unique_rows = df[self.join_key].nunique()
                uniqueness_ratio = unique_rows / total_rows

                df[self.join_key].to_csv(f"data/directories_{df.index.name or 'index'}.csv", index=False)
                logging.debug(f"DataFrame {i} has {unique_rows} unique rows out of {total_rows} total rows. Uniqueness ratio: {uniqueness_ratio:.2f}")
                if uniqueness_ratio > 0.9:
                    candidates.append((i, df.copy(), uniqueness_ratio))

            if not candidates:
                logging.error("No table with nearly-unique key found")

            candidates.sort(key=lambda x: x[2], reverse=True)
            original_index, primary_df, _ = candidates[0]

            primary_df = primary_df.drop_duplicates(subset=[self.join_key], keep='first')
            dfs.pop(original_index)
            dfs.insert(0, primary_df)

            return dfs

        ordered_dfs = identify_primary_df(pandas_dfs)

        merged_df = pd.merge(ordered_dfs[0], ordered_dfs[1],
                             on=self.join_key,
                             how='outer',
                             suffixes=('', '_dup'))

        dup_cols = [col for col in merged_df.columns if col.endswith('_dup')]
        merged_df = merged_df.drop(columns=dup_cols)

        for df in ordered_dfs[2:]:
            if self.join_key not in df.columns:
                raise KeyError(f"Join key '{self.join_key}' not found in DataFrame")

            original_cols = set(merged_df.columns)

            if self.aux_key is not None and self.aux_key in df.columns:
                merged_df = pd.merge(merged_df, df,
                                     on=[self.join_key, self.aux_key],
                                     how='outer',
                                     suffixes=('', '_dup'))
            else:
                merged_df = pd.merge(merged_df, df,
                                     on=self.join_key,
                                     how='outer',
                                     suffixes=('', '_dup'))

            new_dup_cols = [col for col in merged_df.columns
                            if col.endswith('_dup') and
                            col.replace('_dup', '') in original_cols]
            merged_df = merged_df.drop(columns=new_dup_cols)

        return merged_df
