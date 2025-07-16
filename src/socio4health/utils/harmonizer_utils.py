import re
import os
import torch
import numpy as np
import pandas as pd
from transformers import pipeline, Pipeline
from deep_translator import GoogleTranslator


def standardize_dict(raw_dict: pd.DataFrame) -> pd.DataFrame:
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
    df.drop_duplicates(inplace=True)
    grouped_df = df.groupby(['question', 'variable_name'], group_keys=False)\
               .apply(_process_group, include_groups=True)\
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

def translate_column(data: pd.DataFrame, column: str, language: str = 'en') -> pd.DataFrame:
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
    
    data = data.copy()

    new_col = f"{column}_{language}"
    data[new_col] = data[column].apply(
        lambda x: GoogleTranslator(source='auto', target=language).translate(x) if pd.notna(x) else x
    )
    print(f"{column} translated")

    return data

_classifier = None

def get_classifier(MODEL_PATH: str) -> Pipeline:
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
        device = 0 if torch.cuda.is_available() else -1
        _classifier = pipeline("text-classification", model=MODEL_PATH, tokenizer=MODEL_PATH, device=device)
    return _classifier

def classify_rows(data: pd.DataFrame, col1: str, col2: str, col3: str, new_column_name: str = "category",
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
