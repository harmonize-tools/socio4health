import re
import os

import numpy as np
import pandas as pd
from typing import Any
from .deps import import_optional
from .log import logger


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
    group_cols = ['question', 'variable_name']
    # Select only the non-grouping columns: pandas no longer passes the grouping columns to apply().
    # _process_group restores 'question' and 'variable_name' from group.name.
    value_cols = [c for c in df.columns if c not in group_cols]
    grouped_df = df.groupby(group_cols, group_keys=False)[value_cols]\
               .apply(_process_group)\
               .reset_index(drop=True)
    column_order = list(df.columns) + [c for c in grouped_df.columns if c not in df.columns]
    return grouped_df[column_order]

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

_TRANSLATION_MODEL_NAME = "Helsinki-NLP/opus-mt-ROMANCE-en"
_TRANSLATION_MAX_TARGET_TOKENS = 512
_translation_runtime = None
_translation_cache = {}


def _get_translation_runtime():
    """Load and cache the offline translation model and tokenizer."""
    global _translation_runtime
    if _translation_runtime is None:
        torch = import_optional('torch', extra='ml')
        transformers = import_optional('transformers', extra='ml')
        import_optional('sentencepiece', extra='ml')
        tokenizer = transformers.AutoTokenizer.from_pretrained(
            _TRANSLATION_MODEL_NAME
        )
        model = transformers.AutoModelForSeq2SeqLM.from_pretrained(
            _TRANSLATION_MODEL_NAME
        )
        device = torch.device(
            'cuda' if getattr(torch, 'cuda', None) and torch.cuda.is_available() else 'cpu'
        )
        model.to(device)
        model.eval()
        _translation_runtime = torch, tokenizer, model, device
    return _translation_runtime


def _translate_texts(texts: list[str], batch_size: int = 32) -> list[str]:
    """Translate Portuguese or Spanish texts to English in local inference batches."""
    if not texts:
        return []

    torch, tokenizer, model, device = _get_translation_runtime()
    max_source_tokens = min(
        getattr(tokenizer, 'model_max_length', 512),
        getattr(model.config, 'max_position_embeddings', 512),
        512,
    ) - 2

    chunks = []
    chunk_lengths = []
    owners = []
    for owner, text in enumerate(texts):
        token_ids = tokenizer.encode(text, add_special_tokens=False)
        if not token_ids:
            chunks.append(text)
            chunk_lengths.append(1)
            owners.append(owner)
            continue
        for start in range(0, len(token_ids), max_source_tokens):
            chunk_ids = token_ids[start:start + max_source_tokens]
            chunks.append(tokenizer.decode(chunk_ids, skip_special_tokens=True))
            chunk_lengths.append(len(chunk_ids))
            owners.append(owner)

    # Similar lengths reduce padding and prevent short labels from inheriting the
    # generation budget needed by a much longer text in the same batch.
    sorted_indexes = sorted(range(len(chunks)), key=chunk_lengths.__getitem__)
    translated_chunks = [None] * len(chunks)
    for start in range(0, len(sorted_indexes), batch_size):
        batch_indexes = sorted_indexes[start:start + batch_size]
        batch = [chunks[index] for index in batch_indexes]
        encoded = tokenizer(
            batch,
            return_tensors='pt',
            padding=True,
            truncation=True,
            max_length=max_source_tokens,
        )
        encoded = {name: values.to(device) for name, values in encoded.items()}
        longest_source = int(encoded['attention_mask'].sum(dim=1).max().item())
        max_target_tokens = min(
            _TRANSLATION_MAX_TARGET_TOKENS,
            max(32, longest_source * 2 + 16),
        )
        with torch.inference_mode():
            generated = model.generate(
                **encoded,
                max_length=max_target_tokens,
                no_repeat_ngram_size=3,
                repetition_penalty=1.1,
                forced_eos_token_id=tokenizer.eos_token_id,
            )
        decoded = tokenizer.batch_decode(generated, skip_special_tokens=True)
        for index, translated in zip(batch_indexes, decoded):
            translated_chunks[index] = _clean_translation_output(translated)

    if any(translated is None for translated in translated_chunks):
        raise RuntimeError("The translation model did not return all requested texts.")

    grouped = [[] for _ in texts]
    for owner, translated in zip(owners, translated_chunks):
        grouped[owner].append(translated.strip())
    return [' '.join(parts).strip() for parts in grouped]


def _clean_translation_output(text: str) -> str:
    """Remove terminal repetition occasionally produced by sequence generation."""
    text = text.strip()
    text = re.sub(
        r'(?i)\b([\w-]+)(?:(?:\s*,\s*|\s+)\1\b){2,}',
        r'\1',
        text,
    )
    return re.sub(
        r'(?:\s*\.\s*){3,}$',
        '.',
        text,
    ).strip()


def s4h_translate_column(data: pd.DataFrame, column: str, language: str = 'en') -> pd.DataFrame:
    """
    Translates Portuguese or Spanish content in a DataFrame to English using a local model.

    The model is downloaded on first use and then reused from the local Hugging Face
    cache. Repeated values are translated only once per Python process.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the text columns.

    column : str
        Name of the column to translate.

    language : str
        Target language code. The bundled translation model currently supports
        English (``en``) as its target language.

    Returns
    -------
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

    if language.lower() != 'en':
        raise ValueError("The offline translation model only supports English ('en') as target language.")

    data = data.copy()
    new_col = f"{column}_{language}"

    prepared = {}
    truncated = False
    for value in data[column]:
        if pd.isna(value):
            continue
        text = str(value)
        if len(text) >= 5000:
            text = text[:4500]
            truncated = True
        prepared[value] = text

    if truncated:
        logger.warning("Rows with contents longer than 5000 characters are cut off")

    pending = []
    for text in dict.fromkeys(prepared.values()):
        cache_key = (_TRANSLATION_MODEL_NAME, text)
        if text.strip() and cache_key not in _translation_cache:
            pending.append(text)

    if pending:
        translations = _translate_texts(pending)
        for text, translation in zip(pending, translations):
            _translation_cache[(_TRANSLATION_MODEL_NAME, text)] = translation

    def translated_value(value):
        if pd.isna(value):
            return value
        text = prepared[value]
        if not text.strip():
            return text
        return _translation_cache[(_TRANSLATION_MODEL_NAME, text)]

    data[new_col] = data[column].apply(translated_value)
    logger.info(f"{column} translated")

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

def s4h_classify_rows(data: pd.DataFrame, col1: str, col2: str=None, col3: str=None, new_column_name: str = "category",
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
    
    text_columns = [c for c in (col1, col2, col3) if c is not None]

    for col in text_columns:
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
        valid_parts = []
        for col in text_columns:
            x = row[col]
            if isinstance(x, str):
                x = x.strip()
                if x and x.lower() != "not applicable":
                    valid_parts.append(x)
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
    """Clean column names and keep only the first column of each cleaned name.

    Duplicates are removed by position: dropping them by name removed every copy, so a column such as
    ``AREA`` disappeared completely when two columns ended up with that name.
    """
    normalized = [_clean_column_name(col) for col in df.columns]
    keep = []
    seen = set()
    for i, name in enumerate(normalized):
        if name not in seen:
            seen.add(name)
            keep.append(i)

    df = df.iloc[:, keep]  # new object, so renaming below never changes the caller's DataFrame
    df.columns = [normalized[i] for i in keep]
    return df


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

    # Integer codes are compared as numbers: "02", "2", "2.0" and 2 are the same code. Fixed-width files
    # keep leading zeros ("02") while JSON mappings loaded with int keys lose them (2), so without this
    # they never matched.
    if re.fullmatch(r'[-+]?\d+\.0+', text):
        return str(int(text.split('.', 1)[0]))
    if re.fullmatch(r'[-+]?\d+', text):
        return str(int(text))

    lowered = text.lower()
    if lowered in {'na', 'n/a', 'nan', 'none', 'null'}:
        return '0'

    return text


def merge_factor(dfs, factor_col, id_col, fallback_col=None):
    """Give the expansion factor to the tables that do not have it, joining on an ID.

    Parameters
    ----------
    dfs : list of DataFrames
        Tables of one survey year (Dask DataFrames are computed).
    factor_col : str
        Expansion factor column (e.g. ``FEX_C18``).
    id_col : str or list of str
        Column(s) that identify the unit the factor belongs to (e.g. ``DIRECTORIO``, or
        ``["CONGLOME", "VIVIENDA", "HOGAR"]`` when no single column is unique).
    fallback_col : str, optional
        Factor used for rows that still have no ``factor_col`` (e.g. ``FEX_C`` in GEIH 2020: the CNPV
        2018 projection file that gives ``FEX_C18`` does not cover the departments added that year).

    Returns
    -------
    list of pandas.DataFrame
        The tables with normalized column names. Tables that already have ``factor_col`` keep their own
        values; the others get it from every table that has both the ID and the factor.

    Note
    ----
    Earlier versions took the factor from the first table only and replaced the factor of every other
    table with it, so rows from other files (e.g. other months) lost their weight.
    """
    if id_col is None:
        return dfs

    factor_col = _clean_column_name(factor_col)
    id_cols = [id_col] if isinstance(id_col, str) else list(id_col)
    id_cols = [_clean_column_name(c) for c in id_cols]

    dfs = [_normalize_columns(df.compute() if hasattr(df, 'compute') else df) for df in dfs]

    sources = [df[id_cols + [factor_col]] for df in dfs
               if factor_col in df.columns and all(c in df.columns for c in id_cols)]
    out = dfs
    if sources:
        lookup = pd.concat(sources, ignore_index=True).dropna(subset=[factor_col]).drop_duplicates(id_cols)
        out = []
        for df in dfs:
            if factor_col not in df.columns and all(c in df.columns for c in id_cols):
                df = df.merge(lookup, on=id_cols, how='left')
            out.append(df)

    if fallback_col is not None:
        fallback_col = _clean_column_name(fallback_col)
        filled = []
        for df in out:
            if fallback_col in df.columns:
                if factor_col in df.columns:
                    df = df.assign(**{factor_col: df[factor_col].where(df[factor_col].notna(), df[fallback_col])})
                else:
                    df = df.assign(**{factor_col: df[fallback_col]})
            filled.append(df)
        out = filled
    return out


def use_person_factor(dfs, factor_col, person_factor_col, person_id_col):
    """Weight person-level tables with the person (population) factor instead of the household one.

    Some surveys publish a household factor and a separate person factor calibrated to the population
    (ENAHO: ``FACTOR07`` and ``FACPOB07``, the latter only in the household-members module). Person
    variables should use the person factor. This gives ``person_factor_col`` to every table that has the
    person key (joined from the tables that have it) and stores it in ``factor_col``, so the rest of the
    pipeline keeps using a single weight column. Tables without the person key (households) keep their
    own ``factor_col``. Persons without a person factor (e.g. non-residents) are left without weight.

    Parameters
    ----------
    dfs : list of DataFrames
    factor_col : str
        Weight column used by the pipeline (e.g. ``FACTOR07``).
    person_factor_col : str
        Person factor (e.g. ``FACPOB07``).
    person_id_col : list of str
        Columns identifying a person (e.g. ``["CONGLOME", "VIVIENDA", "HOGAR", "CODPERSO"]``).

    Returns
    -------
    list of pandas.DataFrame
    """
    factor_col = _clean_column_name(factor_col)
    person_factor_col = _clean_column_name(person_factor_col)
    id_cols = [_clean_column_name(c) for c in ([person_id_col] if isinstance(person_id_col, str) else person_id_col)]
    dfs = [_normalize_columns(df.compute() if hasattr(df, 'compute') else df) for df in dfs]

    is_person = [all(c in df.columns for c in id_cols) for df in dfs]
    persons = merge_factor([df for df, p in zip(dfs, is_person) if p], person_factor_col, id_cols)
    persons = iter(persons)

    out = []
    for df, p in zip(dfs, is_person):
        if p:
            df = next(persons)
            df = df.drop(columns=[c for c in (factor_col,) if c in df.columns])
            if person_factor_col in df.columns:
                df = df.rename(columns={person_factor_col: factor_col})
            else:
                df = df.assign(**{factor_col: pd.NA})
        out.append(df)
    return out


def prune_columns(dfs, keep):
    """Keep only the given columns (compared by cleaned name) in each table, without computing anything.

    Survey files have hundreds of columns while harmonization uses a few. Pruning Dask DataFrames before
    they are computed means the other columns are never loaded. Call it after
    :meth:`Harmonizer.s4h_vertical_merge`, which needs the full column lists to recognise each table.

    Parameters
    ----------
    dfs : list of DataFrames
    keep : iterable of str
        Column names to keep: raw names of the mapped variables, IDs, weights, ``YEAR``...

    Returns
    -------
    list of DataFrames
        The tables with only the kept columns. Tables left with nothing but ``YEAR`` are removed.
    """
    keep = {_clean_column_name(c) for c in keep}
    out = []
    for df in dfs:
        cols = [c for c in df.columns if _clean_column_name(c) in keep]
        if any(_clean_column_name(c) != 'YEAR' for c in cols):
            out.append(df[cols])
    return out


COALESCE_SEPARATOR = '|'


def mapping_source_columns(column_mapping):
    """Raw columns used by a column mapping, splitting coalesce keys (``"WS2|WS1"`` -> ``WS2``, ``WS1``)."""
    columns = []
    for key in column_mapping:
        for col in str(key).split(COALESCE_SEPARATOR):
            if col and col not in columns:
                columns.append(col)
    return columns


def s4h_coalesce_columns(df, sources, target, keep=()):
    """Create ``target`` from several columns: the first non-empty value, in the order of ``sources``.

    Useful when a question was asked in two forms, e.g. MICS asks the household-use water source
    (``WS2``) only when drinking water is bottled; the other households keep the drinking-water
    source (``WS1``). Empty strings count as missing. The source columns are dropped.

    Parameters
    ----------
    df : pandas.DataFrame
    sources : list of str
        Columns in priority order (compared by cleaned name); missing ones are ignored.
    target : str
        Name of the new column.
    keep : iterable of str
        Source columns to keep (e.g. because another mapping uses them); the others are dropped.

    Returns
    -------
    pandas.DataFrame
        A copy with ``target`` added, or the DataFrame unchanged if none of the sources exists.
    """
    keep = {_clean_column_name(c) for c in keep}
    present = []
    for src in sources:
        col = next((c for c in df.columns if _clean_column_name(c) == _clean_column_name(src)), None)
        if col is not None and col not in present:
            present.append(col)
    if not present:
        return df
    values = None
    for col in present:
        s = df[col]
        if s.dtype == object or str(s.dtype).startswith('string'):
            s = s.where(s.astype('string').str.strip().ne('').fillna(False))
        values = s if values is None else values.where(values.notna(), s)
    return df.drop(columns=[c for c in present if _clean_column_name(c) not in keep]).assign(**{target: values})


def s4h_code_from_flags(df, flags, target, flag_value="1"):
    """Create ``target`` with the code of the only yes/no column that is "yes".

    Some questionnaires ask "which of these do you use?" as one yes/no question per option, and ask for
    the main option only when more than one is "yes" (e.g. PNAD Continua cooking fuel: ``S01016A1``-``A5``,
    then ``S01016B``). For records with exactly one "yes", ``target`` gets that option's code; otherwise it
    is empty. Combine it with the main-option question with a coalesce key (``"S01016B|<target>"``).

    Parameters
    ----------
    df : pandas.DataFrame
    flags : dict
        ``{column: code}``, the code each yes/no column stands for.
    target : str
        Name of the new column.
    flag_value : str
        Value meaning "yes" (compared as text, ``"1"`` by default).

    Returns
    -------
    pandas.DataFrame
        A copy with ``target`` added, or the DataFrame unchanged if none of the flag columns exists.
    """
    present = {}
    for col, code in flags.items():
        match = next((c for c in df.columns if _clean_column_name(c) == _clean_column_name(col)), None)
        if match is not None:
            present[match] = str(code)
    if not present:
        return df
    yes = pd.DataFrame({col: df[col].astype('string').str.strip().str.replace(r'\.0+$', '', regex=True).eq(str(flag_value))
                        for col in present}).fillna(False)
    single = yes.sum(axis=1) == 1
    codes = yes.idxmax(axis=1).map(present).where(single)
    return df.assign(**{target: codes})


def s4h_code_from_rules(df, rules, target):
    """Create ``target`` with the code of the first rule whose conditions a record meets.

    For variables that need several raw columns at once, e.g. completed education from the level reached, the
    last grade approved in it and the title obtained (GEIH ``P3042``, ``P3042S1``, ``P3043``). Each rule is
    ``{"when": {column: [codes]}, "code": code}``; a record meets it when every column has one of the listed
    codes (as in ``keep_rows_any``). Rules are checked in order and the first one met gives the code; records
    meeting none stay empty.

    Parameters
    ----------
    df : pandas.DataFrame
    rules : list of dict
        ``[{"when": {column: [codes]}, "code": code}, ...]`` in priority order.
    target : str
        Name of the new column.

    Returns
    -------
    pandas.DataFrame
        A copy with ``target`` added, or the DataFrame unchanged if it lacks any column the rules use.
    """
    names = {_clean_column_name(c): c for c in df.columns}
    used = {_clean_column_name(c) for rule in rules for c in rule["when"]}
    if not used <= set(names):
        return df
    clean = {c: df[names[c]].astype('string').str.strip().str.replace(r'\.0+$', '', regex=True)
                .str.replace(r'^0+(?=\d)', '', regex=True) for c in used}
    codes = pd.Series(pd.NA, index=df.index, dtype='string')
    for rule in rules:
        match = pd.Series(True, index=df.index)
        for col, values in rule["when"].items():
            match &= clean[_clean_column_name(col)].isin([str(v) for v in values]).fillna(False)
        codes = codes.mask(codes.isna() & match, str(rule["code"]))
    return df.assign(**{target: codes})


def harmonize_columns_by_year(dfs, year, year_mappings):
    """Rename raw columns to harmonized names using the mapping of the given year.

    A mapping key may list several raw columns separated by ``|`` (e.g. ``"WS2|WS1": "H_WATER"``): the
    harmonized column takes the first non-empty value among them, in that order (see
    :func:`s4h_coalesce_columns`). Use the same key in the value mapping.
    """
    rename_map = _resolve_year_mapping(year_mappings, year)

    if not rename_map:
        logger.warning(f"No mappings found for year {year}")
        return dfs

    harmonized_dfs = []
    for df in dfs:
        if hasattr(df, 'compute'):
            df = df.compute()

        df = _normalize_columns(df)

        for orig_col, harm_col in rename_map.items():
            if COALESCE_SEPARATOR in str(orig_col):
                sources = [_clean_column_name(c) for c in str(orig_col).split(COALESCE_SEPARATOR) if c]
                if any(c in df.columns for c in sources):
                    # raw columns that other mapping keys still need are kept (e.g. P301A: education, and
                    # literacy when P302 was not asked)
                    used_elsewhere = {_clean_column_name(c) for k in rename_map if k != orig_col
                                      for c in str(k).split(COALESCE_SEPARATOR)}
                    df = s4h_coalesce_columns(df, sources, _clean_column_name(harm_col), keep=used_elsewhere)
                    logger.info(f"Coalesced {sources} into {harm_col} for year {year}")
        rename_map_simple = {k: v for k, v in rename_map.items() if COALESCE_SEPARATOR not in str(k)}

        applicable_rename = {}
        for orig_col, harm_col in rename_map_simple.items():
            orig_col_normalized = _clean_column_name(orig_col)
            if orig_col_normalized in df.columns:
                applicable_rename[orig_col_normalized] = harm_col

        # A raw column can already carry a harmonized name (GEIH has its own AREA, the metropolitan area
        # code, while CLASE is mapped to AREA). The mapped column wins: drop the raw one so the rename
        # does not create two columns with the same name.
        targets = {_clean_column_name(t) for t in applicable_rename.values()}
        clashing = [c for c in df.columns if c in targets and c not in applicable_rename]
        if clashing:
            df = df.drop(columns=clashing)
            logger.info(f"Dropped raw columns replaced by mapped ones for year {year}: {clashing}")

        if applicable_rename:
            df = df.rename(columns=applicable_rename)
            logger.info(f"Renamed columns for year {year}: {applicable_rename}")

        harmonized_dfs.append(df)

    return harmonized_dfs


def _memoized(func):
    """``func`` computed once per distinct value: a column has millions of values but few distinct codes."""
    cache = {}

    def wrapper(value):
        if value is None or value is pd.NA or (isinstance(value, float) and value != value):
            key = ('missing', type(value))
        else:
            key = (type(value), value)   # 1, 1.0 and True compare equal but may give different results
        try:
            return cache[key]
        except KeyError:
            cache[key] = result = func(value)
            return result
        except TypeError:   # unhashable value
            return func(value)

    return wrapper


def apply_value_mappings(dfs, year, value_mappings, column_aliases=None):
    year_value_map = _resolve_year_mapping(value_mappings, year)
    year_column_map = _resolve_year_mapping(column_aliases, year)

    if not year_value_map:
        logger.warning(f"No value mappings found for year {year}")
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
                normalized_series = df[col_match].map(_memoized(_normalize_value_token))

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
                logger.info(f"Applied value mapping for {col_match} in year {year}")

        mapped_dfs.append(df)

    return mapped_dfs


__all__ = [
    '_clean_column_name',
    '_normalize_columns',
    'merge_factor',
    'prune_columns',
    'use_person_factor',
    'harmonize_columns_by_year',
    'mapping_source_columns',
    's4h_coalesce_columns',
    's4h_code_from_flags',
    's4h_code_from_rules',
    'apply_value_mappings',
]
