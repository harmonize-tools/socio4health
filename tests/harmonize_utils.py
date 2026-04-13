import pandas as pd
from socio4health import Extractor, Harmonizer

def extract_and_prepare_data(year, path, ext, sep, output_path):
    print(f"{year}: {path}")
    extractor = Extractor(
        input_path=path,
        down_ext=[ext],
        sep=sep,
        output_path=output_path
    )
    dfs_extracted = extractor.s4h_extract()
    for df in dfs_extracted:
        df['YEAR'] = year
    return dfs_extracted

def merge_factor(dfs, factor_col, id_col):
    if id_col is None:
        return dfs  # No merge needed
    pos = None
    for i, df in enumerate(dfs):
        if hasattr(df, "compute"):
            df = df.compute()
        if factor_col in df.columns:
            pos = i
            break
    if pos is not None and len(dfs) > pos and factor_col in dfs[pos].columns and id_col in dfs[pos].columns:
        factor_df = dfs[pos][[id_col, factor_col]].drop_duplicates(id_col)
        for i, df in enumerate(dfs):
            if id_col in df.columns:
                if factor_col in df.columns:
                    df = df.drop(columns=[factor_col])
                df = df.merge(factor_df, on=id_col, how='left')
                dfs[i] = df
    return dfs

def select_and_filter_columns(dfs, col_cols, num_cols_threshold):
    dfs = [df[[col for col in col_cols if col in df.columns]] for df in dfs]
    dfs = [df for df in dfs if len(df.columns) > num_cols_threshold]
    return dfs

def group_and_onehot_encode(dfs, group_col, weight_col, id_col):
    grouped_dfs = []
    for i, df in enumerate(dfs):
        if hasattr(df, "compute"):
            df = df.compute()
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
