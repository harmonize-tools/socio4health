import re
import pandas as pd
from socio4health import Extractor, Harmonizer

# HARMONIZED_MAPPING is imported inside functions where runtime sys.path includes 'tests'


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

def extract_and_prepare_data(year, path, ext, sep=None, output_path=None, colnames=None, colspecs=None, on_bad_lines='warn'):
    print(f"{year}: {path}")
    extractor = Extractor(
        input_path=path,
        down_ext=[ext],
        sep=sep,
        output_path=output_path,
        colnames=colnames,
        colspecs=colspecs,
        on_bad_lines=on_bad_lines
    )
    dfs_extracted = extractor.s4h_extract()
    for df in dfs_extracted:
        df['YEAR'] = year
    return dfs_extracted

def merge_factor(dfs, factor_col, id_col):
    if id_col is None:
        return dfs  # No merge needed

    factor_col = _clean_column_name(factor_col)
    id_col = _clean_column_name(id_col)

    pos = None
    for i, df in enumerate(dfs):
        if hasattr(df, "compute"):
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

def group_and_onehot_encode(dfs, group_col, weight_col, id_col):
    grouped_dfs = []
    group_col = _clean_column_name(group_col)
    weight_col = _clean_column_name(weight_col)
    if id_col is not None:
        id_col = _clean_column_name(id_col)

    for i, df in enumerate(dfs):
        if hasattr(df, "compute"):
            df = df.compute()
        df = _normalize_columns(df)

        # Preprocess group column to keep only integer part (e.g., '1.1' -> 1)
        if group_col in df.columns:
            def _keep_int_part(val):
                if pd.isna(val):
                    return val
                s = str(val).strip()
                # match leading integer
                m = re.match(r"([+-]?\d+)", s)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        return s
                # fallback: split on dot and take left part if numeric
                if '.' in s:
                    left = s.split('.', 1)[0]
                    if re.fullmatch(r"[+-]?\d+", left):
                        return int(left)
                return s

            df[group_col] = df[group_col].map(_keep_int_part)

        if group_col in df.columns and weight_col in df.columns:
            df = df.drop(columns=[id_col], errors='ignore')
            df[weight_col] = pd.to_numeric(df[weight_col], errors='coerce')
            group_cols = [group_col]
            if 'YEAR' in df.columns:
                group_cols.append('YEAR')
            cat_cols = [col for col in df.columns if col not in [group_col, weight_col, 'YEAR']]
            dummies = [pd.get_dummies(df[col], prefix=col) for col in cat_cols]
            if dummies:
                dummies_df = pd.concat(dummies, axis=1)
                dummies_df.columns = [c.replace('.0', '') for c in dummies_df.columns]

                # Load harmonized mapping at runtime and translate dummy column suffixes (e.g., AREA_1)
                try:
                    from rd_year_mappings import HARMONIZED_MAPPING
                except Exception:
                    HARMONIZED_MAPPING = {}

                rename_map = {}
                for colname in dummies_df.columns:
                    if '_' not in colname:
                        continue
                    prefix, token = colname.rsplit('_', 1)
                    clean_prefix = _clean_column_name(prefix)
                    harm_info = HARMONIZED_MAPPING.get(clean_prefix)
                    if not isinstance(harm_info, dict):
                        continue
                    values_map = harm_info.get('VALUES')
                    if not isinstance(values_map, dict):
                        continue

                    # Try token directly, then try numeric normalization
                    label = values_map.get(token)
                    if label is None:
                        # If mapping keys are ints, try integer lookup
                        if re.fullmatch(r"[-+]?\d+(?:\.0+)?", token):
                            norm_str = str(int(float(token)))
                            # try string key
                            label = values_map.get(norm_str)
                            # try integer key
                            if label is None:
                                try:
                                    label = values_map.get(int(norm_str))
                                except Exception:
                                    pass
                    if label:
                        # Make safe column suffix
                        label_clean = re.sub(r"\W+", "_", label).strip('_')
                        new_name = f"{prefix}_{label_clean}"
                        rename_map[colname] = new_name

                if rename_map:
                    dummies_df = dummies_df.rename(columns=rename_map)
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
                df_grouped = df_grouped.rename(columns={weight_col: f"{weight_col}_sum"})
                if dummy_cols:
                    total_weight = df_grouped[f"{weight_col}_sum"].replace(0, pd.NA)
                    for col in dummy_cols:
                        df_grouped[col] = df_grouped[col].div(total_weight).fillna(0)
                df_grouped = df_grouped.drop(columns=[f"{weight_col}_sum"])
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
    """
    Aplica mapeos de columnas específicos del año para armonizar variables.
    
    Renombra columnas de acuerdo a los mapeos definidos para cada año,
    normalizando así variables que tienen nombres diferentes en distintos años.
    
    Parameters
    ----------
    dfs : list of pd.DataFrame
        DataFrames a procesar
    year : int
        Año de los datos
    year_mappings : dict
        Diccionario con mapeos: {year: {col_original: col_harmonizado}}
        
    Returns
    -------
    list of pd.DataFrame
        DataFrames con columnas renombradas según el mapeo del año
    """
    rename_map = year_mappings.get(year, {})
    
    if not rename_map:
        print(f"No mappings found for year {year}")
        return dfs
    
    harmonized_dfs = []
    for df in dfs:
        if hasattr(df, "compute"):
            df = df.compute()
        
        df = _normalize_columns(df)
        
        # Crear mapeo solo para columnas que existen en el dataframe
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


def apply_value_mappings(dfs, year, value_mappings):
    """
    Aplica mapeos de valores para normalizar categorías que cambian por año.
    
    Reemplaza valores según los mapeos definidos para cada año,
    normalizando así las opciones de variables categóricas.
    
    Parameters
    ----------
    dfs : list of pd.DataFrame
        DataFrames a procesar
    year : int
        Año de los datos
    value_mappings : dict
        Diccionario con mapeos: {year: {variable: {valor_original: valor_harmonizado}}}
        
    Returns
    -------
    list of pd.DataFrame
        DataFrames con valores remapeados
    """
    year_value_map = value_mappings.get(year, {})

    # Allow applying value maps after column harmonization (e.g. V101 -> H_TYPE).
    try:
        from rd_year_mappings import COLUMN_MAPPING_BY_YEAR
        year_column_map = COLUMN_MAPPING_BY_YEAR.get(year, {})
    except Exception:
        year_column_map = {}

    try:
        from rd_year_mappings import HARMONIZED_MAPPING
    except Exception:
        HARMONIZED_MAPPING = {}
    
    if not year_value_map:
        print(f"No value mappings found for year {year}")
        return dfs
    
    def _normalize_value_token(value):
        if pd.isna(value):
            return None

        # Normalize numeric representations: 96, 96.0, " 96 " -> "96"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        if isinstance(value, int):
            return str(value)

        text = str(value).strip()
        if not text:
            return None

        if text in {"_", ".", "-"}:
            return "0"

        if re.fullmatch(r"[-+]?\d+\.0+", text):
            return text.split(".", 1)[0]
        if re.fullmatch(r"[-+]?\d+", text):
            return text

        lowered = text.lower()
        if lowered in {"na", "n/a", "nan", "none", "null"}:
            return "0"

        return text

    mapped_dfs = []
    for df in dfs:
        if hasattr(df, "compute"):
            df = df.compute()
        
        df = df.copy()
        
        for variable, value_map in year_value_map.items():
            variable_upper = _clean_column_name(variable)
            harmonized_name = year_column_map.get(variable, variable)
            harmonized_upper = _clean_column_name(harmonized_name)

            # Build lookup with normalized keys so map works across int/float/str values.
            normalized_value_map = {}
            for key, mapped_value in value_map.items():
                norm_key = _normalize_value_token(key)
                if norm_key is not None:
                    # Coerce mapped values that are NaN/None to the harmonized '0' (Missing)
                    if pd.isna(mapped_value):
                        mapped_value_norm = "0"
                    else:
                        # Preserve strings, otherwise coerce to string
                        mapped_value_norm = mapped_value if isinstance(mapped_value, str) else str(mapped_value)
                    normalized_value_map[norm_key] = mapped_value_norm

            # Find either original or harmonized column (case-insensitive).
            col_match = None
            for col in df.columns:
                current = _clean_column_name(col)
                if current == variable_upper or current == harmonized_upper:
                    col_match = col
                    break

            if col_match:
                normalized_series = df[col_match].map(_normalize_value_token)

                # Keep original values when no mapping exists after normalization.
                mapped_series = normalized_series.map(
                    lambda x: normalized_value_map.get(x, x)
                )

                # If the source value was NaN (normalized to None) ensure it becomes
                # the harmonized missing token '0' so it is handled consistently.
                mapped_series = mapped_series.fillna("0")

                # If normalization changed placeholders (e.g., '_' -> '0') and that key is not
                # explicit in mapping, keep the normalized token as-is instead of the raw token.
                df[col_match] = mapped_series
                print(f"Applied value mapping for {col_match} in year {year}")

        # Enforce harmonized categorical schema: values outside allowed categories
        # are sent to 0 (Missing), which avoids residual one-hot columns like _96/_99.
        for col in df.columns:
            clean_col = _clean_column_name(col)
            harm_info = HARMONIZED_MAPPING.get(clean_col)
            if not isinstance(harm_info, dict):
                continue

            allowed_values_raw = harm_info.get("VALUES")
            if not isinstance(allowed_values_raw, dict):
                continue

            allowed_values = {
                token
                for token in (_normalize_value_token(v) for v in allowed_values_raw.keys())
                if token is not None
            }

            # Detect open numeric columns (e.g., P_AGE where only 999 -> Missing is listed)
            open_numeric = False
            try:
                if _clean_column_name(col) == 'P_AGE' and (999 in allowed_values_raw or '999' in allowed_values_raw) and len(allowed_values_raw) == 1:
                    open_numeric = True
            except Exception:
                open_numeric = False

            # Skip if there is no usable schema for this column and it's not open_numeric.
            if not allowed_values and not open_numeric:
                continue

            normalized_series = df[col].map(_normalize_value_token)

            def _clamp_value(x):
                if x is None:
                    return x
                if open_numeric and re.fullmatch(r"\d+", str(x)):
                    return x
                return x if x in allowed_values else "0"

            df[col] = normalized_series.map(_clamp_value)
        
        mapped_dfs.append(df)
    
    return mapped_dfs
