import re
import os

import numpy as np
import pandas as pd
from typing import Any
from .deps import import_optional


def s4h_standardize_dict(raw_dict: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and structures a dictionary-like DataFrame of variables by standardizing
    text fields, grouping possible answers, and removing duplicates.

    Parameters
    ----------
    raw_dict : pd.DataFrame
        DataFrame containing the required columns: ``question``, ``variable_name``,
        ``description``, ``value``, and optionally ``subquestion``.

    Returns
    -------
    `pd.DataFrame <https://pandas.pydata.org/docs/reference/frame.html>`_
        A cleaned and grouped DataFrame by ``question`` and ``variable_name``,
        with an additional column ``possible_answers`` containing concatenated descriptions.
    """

    if not isinstance(raw_dict, pd.DataFrame):
        raise TypeError("raw_dict must be a pandas DataFrame.")

    required_columns = {'question', 'variable_name', 'description', 'value'}
    missing_columns = required_columns - set(raw_dict.columns)
    if missing_columns:
        raise ValueError(f"The following required columns are missing: {missing_columns}")

    if "subquestion" in raw_dict.columns:
        if not raw_dict['subquestion'].apply(lambda x: pd.isna(x) or isinstance(x, str)).all():
            raise TypeError("The column 'subquestion' must contain only strings or NaN values.")

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
    df["description"] = df["description"].astype("object")
    if df["description"].isna().all():
        mask = df["variable_name"].isna() & df["question"].notna()
        df.loc[mask, "description"] = df.loc[mask, "question"]
        df.loc[mask, "question"] = pd.NA

    df['question'] = clean_column(df['question']).ffill()
    cols_to_check = df.columns.difference(['question'])
    df = df[~df[cols_to_check].isna().all(axis=1)]

    if "subquestion" in df.columns:
        mask = df['variable_name'].isna() & df['subquestion'].notna()
        df.loc[mask, 'description'] = df.loc[mask, 'subquestion']
        df.loc[mask, 'subquestion'] = np.nan
        df['variable_name'] = clean_column(df['variable_name']).ffill()
        df['subquestion'] = (
            df.groupby('variable_name', group_keys=False)['subquestion']
            .apply(lambda group: clean_column(group).ffill())
        )
        df['subquestion'] = clean_column(df['subquestion'])
        df['question'] = df['question'] + ' ' + df['subquestion'].fillna('')
        df.drop(columns='subquestion', inplace=True)
    else:
        df['variable_name'] = clean_column(df['variable_name']).ffill()

    df['description'] = clean_column(df['description'])
    if df["value"].isna().all():
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        pat = r'^\s*([+-]?\d+(?:[.,]\d+)?)(?:[\s\-_—–:|/\\]+)?(.*\S)?\s*$'
        mask = df["value"].isna() & df["description"].astype(str).str.match(pat, na=False)
        ext = df.loc[mask, "description"].astype(str).str.extract(pat)
        num_str = ext[0]
        txt_str = ext[1]
        num = pd.to_numeric(num_str.str.replace(",", ".", regex=False), errors="coerce")
        df.loc[mask, "value"] = num
        df.loc[mask, "description"] = (
            txt_str.fillna("").str.strip().replace({"": pd.NA})
        )

    df.drop_duplicates(inplace=True)
    df['variable_name'] = df['variable_name'].str.upper()
    grouped_df = df.groupby(['question', 'variable_name'], group_keys=False)\
               .apply(_process_group)\
               .reset_index(drop=True)
    return grouped_df

def _process_group(group: pd.DataFrame) -> pd.Series:
    """
    Processes a group of rows by combining multiple answer descriptions and
    values for each ``question`` and ``variable_name`` pair.

    Parameters
    ----------
    group: pd.DataFrame
        A subgroup of the original DataFrame, grouped by ``question`` and ``variable_name``.

    Returns
    -------
     `pd.Series <https://pandas.pydata.org/docs/reference/api/pandas.Series.html#pandas.Series>`_ 
        A single summary row with the base description (if available),
        concatenated ``possible_answers``, and joined ``values``.
    """

    required_columns = {'description', 'value'}
    missing = required_columns - set(group.columns)
    if missing:
        raise ValueError(f"The following required columns are missing: {missing}")
    
    if group.empty:
        return None

    group_name = group.name
    if isinstance(group_name, tuple) and len(group_name) == 2:
        question_name, variable_name = group_name
    else:
        question_name = pd.NA
        variable_name = pd.NA

    base_row = group[group['value'].isna()].copy()
    answers = group[group['value'].notna()]
    initial_position = None
    size = None
    if len({'size', 'initial_position'} - set(group.columns)) == 0:
        initial_position = group[group['initial_position'].notna()]['initial_position'].values
        size = group[group['size'].notna()]['size'].values

    possible_answers = '; '.join(answers['description'].astype(str))
    values_concat = '; '.join(answers['value'].astype(str))
    possible_answers = possible_answers if possible_answers else np.nan
    values_concat = values_concat if values_concat else np.nan


    if not base_row.empty:
        row = base_row.iloc[0]
        row['possible_answers'] = possible_answers
        row['value'] = values_concat
        if initial_position is not None:
            row['initial_position'] = initial_position
            row['size'] = size
    else:
        row = group.iloc[0].copy()
        row['description'] = np.nan
        row['value'] = values_concat
        row['possible_answers'] = possible_answers
        if initial_position is not None:
            row['initial_position'] = initial_position
            row['size'] = size

    row['question'] = question_name
    row['variable_name'] = variable_name

    return row

def s4h_translate_column(data: pd.DataFrame, column: str, language: str = 'en') -> pd.DataFrame:
    """
    Translates the content of selected columns in a DataFrame using Google Translate.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the text columns.

    column : str
        Name of the column to translate.

    language : str
        Target language code (default is ``en``).

    Returns
    -------
    `pd.DataFrame <https://pandas.pydata.org/docs/reference/frame.html>`_
        Original DataFrame with new column translated.
    """

    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a pandas DataFrame.")
    
    if not isinstance(column, str):
        raise TypeError("column must be a text string.")
    
    if column not in data.columns:
        raise ValueError(f"The column '{column}' is not found in the DataFrame.")
    
    if not isinstance(language, str) or len(language) != 2:
        raise ValueError("The 'language' parameter must be a 2-letter ISO 639-1 language code (e.g. 'en').")
    
    
    def translate_text(text):
        if pd.isna(text):
            return text
        if len(text) < 5000:
            dt = import_optional('deep_translator', extra='ml')
            return dt.GoogleTranslator(source='auto', target=language).translate(text)
        else:
            print("Rows with contents longer than 5000 characters are cut off")
            dt = import_optional('deep_translator', extra='ml')
            return dt.GoogleTranslator(source='auto', target=language).translate(text[:4500])

    data = data.copy()

    new_col = f"{column}_{language}"
    data[new_col] = data[column].apply(translate_text)
    print(f"{column} translated")

    return data

_classifier = None

def s4h_get_classifier(MODEL_PATH: str) -> Any:
    """
    Load the ``BERT`` fine-tuned model for classification only once.

    Parameters
    ----------
    MODEL_PATH : str

    Returns
    -------
    Pipeline
        A ``HuggingFace`` pipeline for text classification.

    """

    if not os.path.exists(MODEL_PATH) and "/" not in MODEL_PATH:
        raise ValueError("MODEL_PATH does not appear to be a valid path or HuggingFace model identifier.")

    global _classifier
    if _classifier is None:
        torch = import_optional('torch', extra='ml')
        transformers = import_optional('transformers', extra='ml')
        device = 0 if getattr(torch, 'cuda', None) and torch.cuda.is_available() else -1
        _classifier = transformers.pipeline("text-classification", model=MODEL_PATH, tokenizer=MODEL_PATH, device=device)
    return _classifier

def s4h_classify_rows(data: pd.DataFrame, col1: str, col2: str, col3: str, new_column_name: str = "category",
        MODEL_PATH: str = "./bert_finetuned_classifier") -> pd.DataFrame:
    """
    Classify each row using a fine-tuned multiclass classification ``BERT`` model.
    
    Parameters
    -----------
    data: pd.DataFrame
        The DataFrame with text columns.
    col1: str
        Name of the first column containing survey-related text.
    col2: str
        Name of the second column containing survey-related text.
    col3: str
        Name of the third column containing survey-related text.
    new_column_name: str, optional
        Name of the new column to store the predicted categories (default is
        ``category``).
    MODEL_PATH: str
        Path to the model weights (default is ``./bert_finetuned_classifier``)

    Returns
    --------
    `pd.DataFrame <https://pandas.pydata.org/docs/reference/frame.html>`_ 
        `pd.DataFrame <https://pandas.pydata.org/docs/reference/frame.html>`_ with a new prediction column.
    
    """

    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a pandas.DataFrame.")

    for col in (col1, col2, col3):
        if not isinstance(col, str):
            raise TypeError("The parameters col1, col2 and col3 must be strings.")
        if col not in data.columns:
            raise ValueError(f"The column '{col}' is not found in the DataFrame.")

    if not isinstance(new_column_name, str) or not new_column_name:
        raise ValueError("new_column_name must be a non-empty string.")

    if new_column_name in data.columns:
        raise ValueError(f"The column '{new_column_name}' already exists in the DataFrame.")

    if not isinstance(MODEL_PATH, str):
        raise TypeError("MODEL_PATH must be a text string.")

    classifier = s4h_get_classifier(MODEL_PATH)

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



def _clean_column_name(name):
    cleaned = str(name).strip().upper()
    cleaned = cleaned.replace('\ufeff', '')
    cleaned = cleaned.replace('Ï»¿', '')
    cleaned = cleaned.replace('ï»¿', '')
    return cleaned.strip()


def _normalize_columns(df):
    rename_map = {}
    seen = set()
    drop_cols = []

    for col in df.columns:
        normalized = _clean_column_name(col)
        if normalized in seen:
            drop_cols.append(col)
        else:
            seen.add(normalized)
            rename_map[col] = normalized

    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df.rename(columns=rename_map)


def _resolve_year_mapping(mapping, year):
    if not isinstance(mapping, dict):
        return {}

    year_value = mapping.get(year)
    if isinstance(year_value, dict):
        return year_value

    return mapping


def _normalize_value_token(value):
    if pd.isna(value):
        return None

    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, int):
        return str(value)

    text = str(value).strip()
    if not text:
        return None

    if text in {'_', '.', '-'}:
        return '0'

    if re.fullmatch(r'[-+]?\d+\.0+', text):
        return text.split('.', 1)[0]
    if re.fullmatch(r'[-+]?\d+', text):
        return text

    lowered = text.lower()
    if lowered in {'na', 'n/a', 'nan', 'none', 'null'}:
        return '0'

    return text


def _sanitize_suffix(value, fallback='Missing'):
    cleaned = re.sub(r'\W+', '_', str(value)).strip('_')
    return cleaned or fallback


def _resolve_value_map(value_labels_by_column, column_name):
    if not isinstance(value_labels_by_column, dict):
        return {}

    candidates = [_clean_column_name(column_name), str(column_name)]
    for candidate in candidates:
        if candidate in value_labels_by_column:
            entry = value_labels_by_column[candidate]
            if isinstance(entry, dict) and 'VALUES' in entry and isinstance(entry['VALUES'], dict):
                return entry['VALUES']
            if isinstance(entry, dict):
                return entry
            return {}

    return {}


def _lookup_label(values_map, token_clean):
    if not values_map:
        return None

    normalized_value_map = {}
    for key, mapped_value in values_map.items():
        norm_key = _normalize_value_token(key)
        if norm_key is not None:
            normalized_value_map[norm_key] = mapped_value

    keys_to_try = [token_clean]
    if token_clean.isdigit():
        keys_to_try.append(int(token_clean))
        keys_to_try.append(float(token_clean))

    for key in keys_to_try:
        norm_key = _normalize_value_token(key)
        if norm_key in normalized_value_map:
            return normalized_value_map[norm_key]

    return None


def extract_and_prepare_data(year, path, ext, sep=None, output_path=None, colnames=None, colspecs=None, on_bad_lines='warn'):
    from ..extractor import Extractor

    print(f"{year}: {path}")
    extractor = Extractor(
        input_path=path,
        down_ext=[ext],
        sep=sep,
        output_path=output_path,
        colnames=colnames,
        colspecs=colspecs,
        on_bad_lines=on_bad_lines,
    )
    dfs_extracted = extractor.s4h_extract()
    for df in dfs_extracted:
        df['YEAR'] = year
    return dfs_extracted


def merge_factor(dfs, factor_col, id_col):
    if id_col is None:
        return dfs

    factor_col = _clean_column_name(factor_col)
    id_col = _clean_column_name(id_col)

    pos = None
    for i, df in enumerate(dfs):
        if hasattr(df, 'compute'):
            df = df.compute()
        df = _normalize_columns(df)
        dfs[i] = df
        if factor_col in df.columns:
            pos = i
            break

    if pos is not None and len(dfs) > pos and factor_col in dfs[pos].columns and id_col in dfs[pos].columns:
        factor_df = dfs[pos][[id_col, factor_col]].drop_duplicates(id_col)
        for i, df in enumerate(dfs):
            df = _normalize_columns(df)
            if id_col in df.columns:
                if factor_col in df.columns:
                    df = df.drop(columns=[factor_col])
                df = df.merge(factor_df, on=id_col, how='left')
                dfs[i] = df

    return dfs


def select_and_filter_columns(dfs, col_cols, num_cols_threshold):
    normalized_cols = []
    seen = set()
    for col in col_cols:
        normalized = _clean_column_name(col)
        if normalized not in seen:
            seen.add(normalized)
            normalized_cols.append(normalized)

    normalized_dfs = []
    for df in dfs:
        df = _normalize_columns(df)
        normalized_dfs.append(df[[col for col in normalized_cols if col in df.columns]])

    dfs = normalized_dfs
    dfs = [df for df in dfs if len(df.columns) > num_cols_threshold]
    return dfs


def group_and_onehot_encode(dfs, group_col, weight_col, id_col, value_labels_by_column=None):
    grouped_dfs = []
    group_col = _clean_column_name(group_col)
    weight_col = _clean_column_name(weight_col)
    if id_col is not None:
        id_col = _clean_column_name(id_col)

    for i, df in enumerate(dfs):
        if hasattr(df, 'compute'):
            df = df.compute()
        df = _normalize_columns(df)

        if group_col in df.columns:
            def _keep_int_part(val):
                if pd.isna(val):
                    return val
                s = str(val).strip()
                m = re.match(r'([+-]?\d+)', s)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        return s
                if '.' in s:
                    left = s.split('.', 1)[0]
                    if re.fullmatch(r'[+-]?\d+', left):
                        return int(left)
                return s

            df[group_col] = df[group_col].map(_keep_int_part)

        if group_col in df.columns and weight_col in df.columns:
            df = df.drop(columns=[id_col], errors='ignore')
            df[weight_col] = pd.to_numeric(df[weight_col], errors='coerce')

            df = df.dropna(subset=[group_col])
            df = df[df[group_col].astype(str).str.strip() != '']

            group_cols = [group_col]
            if 'YEAR' in df.columns:
                group_cols.append('YEAR')
            cat_cols = [col for col in df.columns if col not in [group_col, weight_col, 'YEAR']]
            dummies = [pd.get_dummies(df[col], prefix=col) for col in cat_cols]

            if dummies:
                dummies_df = pd.concat(dummies, axis=1)
                dummies_df.columns = [c.replace('.0', '') for c in dummies_df.columns]

                rename_map = {}
                for colname in dummies_df.columns:
                    if '_' not in colname:
                        continue

                    prefix, token = colname.rsplit('_', 1)
                    clean_prefix = _clean_column_name(prefix)
                    token_clean = str(token).strip().replace('.0', '')

                    values_map = _resolve_value_map(value_labels_by_column, clean_prefix)
                    label = _lookup_label(values_map, token_clean)

                    if values_map and label is not None and str(label).strip() != '':
                        new_name = f'{prefix}_{_sanitize_suffix(label)}'
                    elif values_map:
                        new_name = f'{prefix}_Missing'
                    else:
                        new_name = f'{prefix}_{_sanitize_suffix(token_clean)}'
                        if new_name == f'{prefix}_':
                            new_name = f'{prefix}_Missing'

                    if new_name != colname:
                        rename_map[colname] = new_name

                if rename_map:
                    dummies_df = dummies_df.rename(columns=rename_map)

                clean_dummy_cols = {}
                for col in dummies_df.columns:
                    if re.search(r'_\s*$', col) or re.search(r'_(nan|none)$', col, re.IGNORECASE):
                        prefix = col.rsplit('_', 1)[0]
                        clean_dummy_cols[col] = f'{prefix}_Missing'

                if clean_dummy_cols:
                    dummies_df = dummies_df.rename(columns=clean_dummy_cols)

                dummies_df = dummies_df.multiply(df[weight_col], axis=0)
                cols_to_concat = [df[[group_col, weight_col]]]
                if 'YEAR' in df.columns:
                    cols_to_concat.insert(1, df[['YEAR']])
                cols_to_concat.append(dummies_df)
                df_onehot = pd.concat(cols_to_concat, axis=1)
                df_grouped = df_onehot.groupby(group_cols).sum(numeric_only=True).reset_index()

                if df_grouped.columns.duplicated().any():
                    group_cols_frame = df_grouped[group_cols]
                    numeric_cols = df_grouped.drop(columns=group_cols).T.groupby(level=0).sum().T
                    df_grouped = pd.concat([group_cols_frame, numeric_cols], axis=1)

                dummy_cols = [col for col in df_grouped.columns if col not in group_cols + [weight_col]]
                df_grouped = df_grouped.rename(columns={weight_col: f'{weight_col}_sum'})

                if dummy_cols:
                    total_weight = df_grouped[f'{weight_col}_sum'].replace(0, pd.NA)
                    for col in dummy_cols:
                        df_grouped[col] = df_grouped[col].div(total_weight).fillna(0)

                df_grouped = df_grouped.drop(columns=[f'{weight_col}_sum'])
                if 'YEAR' in df_grouped.columns:
                    cols = ['YEAR'] + [col for col in df_grouped.columns if col != 'YEAR']
                    df_grouped = df_grouped[cols]

                grouped_dfs.append(df_grouped)
                print(f"One-hot grouped DataFrame {i} by {group_cols} (proportions by total {weight_col}):")
                print(df_grouped.head())
            else:
                print(f"DataFrame {i} no tiene columnas categóricas para one-hot encoding.")
        else:
            print(f"DataFrame {i} does not have '{group_col}' or '{weight_col}', skipping group.")

    return grouped_dfs


def harmonize_columns_by_year(dfs, year, year_mappings):
    rename_map = _resolve_year_mapping(year_mappings, year)

    if not rename_map:
        print(f"No mappings found for year {year}")
        return dfs

    harmonized_dfs = []
    for df in dfs:
        if hasattr(df, 'compute'):
            df = df.compute()

        df = _normalize_columns(df)

        applicable_rename = {}
        for orig_col, harm_col in rename_map.items():
            orig_col_normalized = _clean_column_name(orig_col)
            if orig_col_normalized in df.columns:
                applicable_rename[orig_col_normalized] = harm_col

        if applicable_rename:
            df = df.rename(columns=applicable_rename)
            print(f"Renamed columns for year {year}: {applicable_rename}")

        harmonized_dfs.append(df)

    return harmonized_dfs


def apply_value_mappings(dfs, year, value_mappings, column_aliases=None):
    year_value_map = _resolve_year_mapping(value_mappings, year)
    year_column_map = _resolve_year_mapping(column_aliases, year)

    if not year_value_map:
        print(f"No value mappings found for year {year}")
        return dfs

    mapped_dfs = []
    for df in dfs:
        if hasattr(df, 'compute'):
            df = df.compute()

        df = df.copy()

        for variable, value_map in year_value_map.items():
            variable_upper = _clean_column_name(variable)
            harmonized_name = year_column_map.get(variable, variable)
            harmonized_upper = _clean_column_name(harmonized_name)

            col_match = None
            for col in df.columns:
                current = _clean_column_name(col)
                if current == variable_upper or current == harmonized_upper:
                    col_match = col
                    break

            if col_match and isinstance(value_map, dict):
                normalized_series = df[col_match].map(_normalize_value_token)

                normalized_value_map = {}
                for key, mapped_value in value_map.items():
                    norm_key = _normalize_value_token(key)
                    if norm_key is not None:
                        if pd.isna(mapped_value):
                            mapped_value_norm = '0'
                        else:
                            mapped_value_norm = mapped_value if isinstance(mapped_value, str) else str(mapped_value)
                        normalized_value_map[norm_key] = mapped_value_norm

                mapped_series = normalized_series.map(lambda x: normalized_value_map.get(x, x))
                mapped_series = mapped_series.fillna('0')
                df[col_match] = mapped_series
                print(f"Applied value mapping for {col_match} in year {year}")

        mapped_dfs.append(df)

    return mapped_dfs


__all__ = [
    '_clean_column_name',
    '_normalize_columns',
    'extract_and_prepare_data',
    'merge_factor',
    'select_and_filter_columns',
    'group_and_onehot_encode',
    'harmonize_columns_by_year',
    'apply_value_mappings',
]