import json
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple, Dict, Union, Type
from typing import List
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
from tqdm import tqdm
from deep_translator import GoogleTranslator
import torch
import logging
import re
from socio4health.extractor import Extractor
from socio4health.enums.data_info_enum import NameEnum
from transformers import pipeline

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

def drop_nan_columns(ddf_or_ddfs, threshold=1.0, sample_frac=None):
    """
    Drop columns where the majority of values are NaN.

    Parameters:
    -----------
    ddf_or_ddfs : dask.dataframe.DataFrame or list of dask.dataframe.DataFrame
        Input Dask DataFrame or list of Dask DataFrames
    threshold : float, optional (default=1.0)
        Drop columns with NaN percentage above this threshold (0.0 to 1.0)
    sample_frac : float or None, optional (default=None)
        If specified, uses sampling to estimate NaN percentages (faster for large DataFrames)

    Returns:
    --------
    dask.dataframe.DataFrame or list of dask.dataframe.DataFrame
        DataFrame(s) with columns dropped
    """
    logging.info("Dropping columns with majority NaN values...")

    if not 0 <= threshold <= 1:
        raise ValueError("Threshold must be between 0 and 1")

    def process_ddf(ddf):
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

    if isinstance(ddf_or_ddfs, list):
        return [process_ddf(ddf) for ddf in ddf_or_ddfs]
    else:
        return process_ddf(ddf_or_ddfs)

def get_available_columns(ddfs, translate=False, dictionary=None):
    """
    Get a list of unique column names from a list of Dask DataFrames.

    Parameters:
    -----------
    ddfs : list of dask.dataframe.DataFrame
        List of Dask DataFrames.

    Returns:
    --------
    list
        Sorted list of unique column names.
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
        country_dfs: Dict[str, List[dd.DataFrame]],
        column_mapping: Union[Type[Enum], Dict[str, Dict[str, str]], str, Path],
        value_mappings: Union[Type[Enum], Dict[str, Dict[str, Dict[str, str]]], str, Path],
        theme_info: Optional[Union[Dict[str, List[str]], str, Path]] = None,
        default_country: Optional[str] = None,
        strict_mapping: bool = False
) -> Dict[str, List[dd.DataFrame]]:
    """
    Harmonizes Dask DataFrames using either Enum classes or JSON mappings.

    Parameters:
    -----------
    country_dfs : Dict[str, List[dd.DataFrame]]
        Dictionary of country codes to lists of Dask DataFrames

    column_mapping : Union[Type[Enum], Dict, str, Path]
        Either:
        - An Enum class with country mappings
        - A dictionary {country: {orig_col: harmonized_col}}
        - Path to JSON file with mappings
        - JSON string with mappings

    value_mappings : Union[Type[Enum], Dict, str, Path]
        Either:
        - An Enum class with value mappings
        - A dictionary {country: {col: {orig_val: harmonized_val}}}
        - Path to JSON file
        - JSON string

    theme_info : Optional[Union[Dict, str, Path]]
        Optional theme information in same format options

    default_country : Optional[str]
        Fallback country if mapping not found

    strict_mapping : bool
        If True, raises error when columns/values aren't mapped

    Returns:
    --------
    Dict[str, List[dd.DataFrame]]
        Harmonized DataFrames with same structure as input
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
                if default_country:
                    return mapping_obj[default_country].value
                return {}
        elif isinstance(mapping_obj, dict):
            return mapping_obj.get(country,
                                   mapping_obj.get(default_country, {}))
        return {}

    # Load mappings if they're JSON
    column_mapping = load_mapping(column_mapping)
    value_mappings = load_mapping(value_mappings)
    theme_info = load_mapping(theme_info) if theme_info else None

    def process_dataframe(df: dd.DataFrame, country: str) -> dd.DataFrame:
        """Process a single dataframe"""
        # Get mappings for this country
        col_map = get_country_mapping(column_mapping, country)
        val_maps = get_country_mapping(value_mappings, country)

        # Validate mappings if in strict mode
        if strict_mapping:
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
                if strict_mapping:
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

def standardize_dict(raw_dict):
    """
    Cleans and structures a dictionary-like DataFrame of variables by standardizing
    text fields, grouping possible answers, and removing duplicates.

    Parameters
    ----------
    raw_dict : pd.DataFrame
        DataFrame containing the required columns: 'question', 'variable_name',
        'description', 'value', and optionally 'subquestion'.

    Returns
    -------
    pd.DataFrame
        A cleaned and grouped DataFrame by 'question' and 'variable_name',
        with an additional column 'possible_answers' containing concatenated descriptions.
    """

    def clean_column(column):
        return (
            column.replace(r'^\s*$', np.nan, regex=True)
                .apply(lambda x: (
                    re.sub(r'\s{2,}', ' ',
                    re.sub(r'(\s*\.\s*){2,}', ' ',
                    re.sub(r'[\n\t\r]', ' ',
                    re.sub(r'([¿¡])\s+', r'\1',
                    re.sub(r'\s+([?!:;,\.])', r'\1',
                    re.sub(r'([?!:;,\.])\s+', r'\1 ',
                    str(x).replace('…', ' ').strip().lower()))))))
                ) if pd.notna(x) else np.nan)
        )

    df = raw_dict.copy()

    df['question'] = clean_column(df['question']).fillna(method='ffill')
    cols_to_check = df.columns.difference(['question'])
    df = df[~df[cols_to_check].isna().all(axis=1)]

    if "subquestion" in df.columns:
        mask = df['variable_name'].isna() & df['subquestion'].notna()
        df.loc[mask, 'description'] = df.loc[mask, 'subquestion']
        df.loc[mask, 'subquestion'] = np.nan
        df['variable_name'] = clean_column(df['variable_name']).fillna(method='ffill')
        df['subquestion'] = (
            df.groupby('variable_name', group_keys=False)['subquestion']
            .apply(lambda group: clean_column(group).fillna(method='ffill'))
        )
        df['subquestion'] = clean_column(df['subquestion'])
        df['question'] = df['question'] + ' ' + df['subquestion'].fillna('')
        df.drop(columns='subquestion', inplace=True)
    else:
        df['variable_name'] = clean_column(df['variable_name']).fillna(method='ffill')

    df['description'] = clean_column(df['description'])
    df.drop_duplicates(inplace=True)
    grouped_df = df.groupby(['question', 'variable_name'], as_index=False)\
                    .apply(_process_group)\
                    .reset_index(drop=True)

    return grouped_df

def _process_group(group):
    """
    Processes a group of rows by combining multiple answer descriptions and values
    for each 'question' and 'variable_name' pair.

    Parameters
    ----------
    group : pd.DataFrame
        A subgroup of the original DataFrame, grouped by 'question' and 'variable_name'.

    Returns
    -------
    pd.Series
        A single summary row with the base description (if available),
        concatenated 'possible_answers', and joined 'values'.
    """
    if group.empty:
        return None

    base_row = group[group['value'].isna()].copy()
    answers = group[group['value'].notna()]

    possible_answers = '; '.join(answers['description'].astype(str))
    values_concat = '; '.join(answers['value'].astype(str))
    possible_answers = possible_answers if possible_answers else np.nan
    values_concat = values_concat if values_concat else np.nan

    if not base_row.empty:
        row = base_row.iloc[0]
        row['possible_answers'] = possible_answers
        row['value'] = values_concat
    else:
        row = group.iloc[0].copy()
        row['description'] = np.nan
        row['value'] = values_concat
        row['possible_answers'] = possible_answers

    return row

def translate_column (data, column, language = 'en'):
    """
    Translates the content of selected columns in a DataFrame using Google Translate.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the text columns.

    column : str
        Name of the column to translate.

    language : str
        Target language code (default is 'en').

    Returns
    -------
    pd.DataFrame
        Original DataFrame with new column translated.
    """
    data = data.copy()
    if column not in data.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame.")

    new_col = f"{column}_{language}"
    data[new_col] = data[column].apply(
        lambda x: GoogleTranslator(source='auto', target=language).translate(x) if pd.notna(x) else x
    )
    print(f"{column} translated")

    return data

_classifier = None

def get_classifier(MODEL_PATH):
    """
    Load the BERT fine-tuned model for classification only once.
    """
    global _classifier
    if _classifier is None:
        device = 0 if torch.cuda.is_available() else -1
        _classifier = pipeline("text-classification", model=MODEL_PATH, tokenizer=MODEL_PATH, device=device)
    return _classifier

def classify_rows(data, col1, col2, col3, new_column_name="category",
                  MODEL_PATH = "./bert_finetuned_classifier"):
    """
    Classify each row using a fine-tuned multiclass classification BERT model.

    Parameters:
    -----------
    data : pd.DataFrame
        The DataFrame with text columns.
    col1_name : str
        Name of the first column containing survey-related text.
    col2_name : str
        Name of the second column containing survey-related text.
    col3_name : str
        Name of the third column containing survey-related text.
    new_column_name : str, optional
        Name of the new column to store the predicted categories (default is
        'category').
    MODEL_PATH: str
        Path to the model weights (default is './bert_finetuned_classifier')

    Returns:
    --------
    pd.DataFrame with a new prediction column.
    """
    classifier = get_classifier(MODEL_PATH)

    def classify_row(row):
        valid_parts = [
            str(x).strip()
            for x in [row[col1], row[col2], row[col3]]
            if isinstance(x, str) and x.strip() and x.strip().lower() != "not applicable"
        ]
        if not valid_parts:
            return ""

        combined_text = " ".join(valid_parts)
        result = classifier(combined_text, truncation=True, max_length=128)[0]
        return result["label"]

    df = data.copy()
    df[new_column_name] = df.apply(classify_row, axis=1)

    return df

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