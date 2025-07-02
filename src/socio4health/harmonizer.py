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
    def __init__(self,
                 min_common_columns: int = 1,
                 similarity_threshold: float = 0.8,
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
                 extra_cols: Optional[List[str]] = None):
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

    def vertical_merge(self, ddfs: List[dd.DataFrame]) -> List[dd.DataFrame]:
        """
        Merge a list of Dask DataFrames vertically using instance parameters.
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

        return merged_dfs

    def drop_nan_columns(self, ddf_or_ddfs: Union[dd.DataFrame, List[dd.DataFrame]]) -> Union[
        dd.DataFrame, List[dd.DataFrame]]:
        """
        Drop columns where the majority of values are NaN using instance parameters.
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
    def get_available_columns(ddfs: List[dd.DataFrame]) -> List[str]:
        """
        Get a list of unique column names from a list of Dask DataFrames.
        """
        if not isinstance(ddfs, list):
            raise TypeError("Input must be a list of Dask DataFrames")

        unique_columns = set()
        for ddf in ddfs:
            if not isinstance(ddf, dd.DataFrame):
                raise TypeError("All elements in the list must be Dask DataFrames")
            unique_columns.update(ddf.columns)

        return sorted(unique_columns)

    def harmonize_dataframes(
            self,
            country_dfs: Dict[str, List[dd.DataFrame]]
    ) -> Dict[str, List[dd.DataFrame]]:
        """
        Harmonizes Dask DataFrames using instance parameters.
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

    def data_selector(self, ddfs: List[dd.DataFrame]) -> List[dd.DataFrame]:
        """
        Selects rows from Dask DataFrames using instance parameters.
        """

        dict_df = self.dict_df.copy()
        dict_df['variable_name'] = dict_df['variable_name'].str.upper()
        key_column_upper = self.key_col.upper()

        filtered_ddfs = []
        for ddf in ddfs:
            if self.key_col not in ddf.columns:
                raise KeyError(f"Key column '{self.key_col}' not found in DataFrame")

            filtered_ddf = ddf[ddf[self.key_col].isin(self.key_val)]
            if len(filtered_ddf) == 0:
                logging.warning(f"No rows found matching key values in DataFrame")

            filtered_ddf.columns = filtered_ddf.columns.str.upper()

            dict_df_filtered = dict_df[dict_df[ColumnMappingEnum.CATEGORY.value].isin(self.categories)]
            columns_list = dict_df_filtered[ColumnMappingEnum.VARIABLE_NAME.value].dropna().unique().tolist()

            if self.extra_cols:
                columns_list.extend(col.upper() for col in self.extra_cols if col.upper() not in columns_list)

            if columns_list:
                logging.debug(f"Filtering DataFrame for columns: {columns_list}")
                logging.debug(f"Available columns: {filtered_ddf.columns.tolist()}")

                existing_columns = [
                    col for col in columns_list
                    if col in filtered_ddf.columns and col != key_column_upper
                ]

                final_columns = [key_column_upper] + existing_columns

                if len(final_columns) == 1:
                    logging.warning("Only key column found in the filtered DataFrame")
                filtered_ddf = filtered_ddf[final_columns]
            else:
                logging.warning("No columns found matching the specified categories")
                filtered_ddf = filtered_ddf[[key_column_upper]]

            filtered_ddfs.append(filtered_ddf)

        return filtered_ddfs