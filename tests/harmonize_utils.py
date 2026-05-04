import pandas as pd
from socio4health import Extractor, Harmonizer


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
                dummies_df = dummies_df.multiply(df[weight_col], axis=0)
                cols_to_concat = [df[[group_col, weight_col]]]
                if 'YEAR' in df.columns:
                    cols_to_concat.insert(1, df[['YEAR']])
                cols_to_concat.append(dummies_df)
                df_onehot = pd.concat(cols_to_concat, axis=1)
                df_grouped = df_onehot.groupby(group_cols).sum(numeric_only=True).reset_index()
                df_grouped = df_grouped.rename(columns={weight_col: f"{weight_col}_sum"})
                if 'YEAR' in df_grouped.columns:
                    cols = ['YEAR'] + [col for col in df_grouped.columns if col != 'YEAR']
                    df_grouped = df_grouped[cols]
                grouped_dfs.append(df_grouped)
                print(f"One-hot grouped DataFrame {i} by {group_cols} (weighted sum by {weight_col}):")
                print(df_grouped.head())
            else:
                print(f"DataFrame {i} no tiene columnas categóricas para one-hot encoding.")
        else:
            print(f"DataFrame {i} does not have '{group_col}' or '{weight_col}', skipping group.")
    return grouped_dfs
