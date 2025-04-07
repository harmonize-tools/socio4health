import logging

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transformer:

    def __init__(self, dictionary: pd.DataFrame = None, dataframes: list | pd.DataFrame = None,
                translated_dataframes: list = None, selected_columns: list = None):
        self._dictionary = dictionary
        self._dataframes = dataframes if isinstance(dataframes, list) else [dataframes]
        self._selected_columns = selected_columns
        self._translated_dataframes = translated_dataframes if isinstance(translated_dataframes, list) else [translated_dataframes]

    @property
    def dictionary(self):
        return self._dictionary

    @dictionary.setter
    def dictionary(self, dictionary: pd.DataFrame):
        self._dictionary = dictionary

    @property
    def dataframes(self):
        return self._dataframes

    @dataframes.setter
    def dataframes(self, dataframes: list | pd.DataFrame):
        self._dataframes = dataframes if isinstance(dataframes, list) else [dataframes]

    @property
    def translated_dataframes(self):
        return self._translated_dataframes

    @translated_dataframes.setter
    def translated_dataframes(self, translated_dataframes: list | pd.DataFrame):
        self._translated_dataframes = translated_dataframes if isinstance(translated_dataframes, list) else [translated_dataframes]

    def get_dfs_names(self, dataframes: list[pd.DataFrame]) -> list[str]:
        return [f"df_{i}" for i in range(len(dataframes))]

    def get_columns(self, dataframes: list[pd.DataFrame]) -> list[str]:
        unique_columns = set()
        for df in dataframes:
            unique_columns.update(df.columns)
        return list(unique_columns)

    def translate(self):
        logging.info("----------------------")
        logging.info("Starting data translation...")
        try:
            if self.dictionary is None:
                logging.error("No dictionary was provided for translation.")
                raise ValueError("No dictionary was provided for translation.")
            if self.raw_dataframes is None:
                logging.error("No raw dataframes were provided for translation.")
                raise ValueError("No raw dataframes were provided for translation.")
            #translate body
            logging.info("Translation completed successfully.")
        except Exception as e:
            logging.error(f"Exception while translating data: {e}")
            raise ValueError(f"Translation failed: {str(e)}")
        return self.translated_dataframes

    def vertical_merge(self, fill_value=None, sort_columns=False, similarity_threshold=1, nan_threshold=1, drop_empty_rows=True, fill_method=None):
        """
        Vertically merge DataFrames by first clustering similar ones together.

        Parameters:
        - fill_value: Value for missing columns (default None)
        - sort_columns: Whether to sort columns alphabetically (default False)
        - similarity_threshold: Minimum column similarity score (0-1) to cluster DataFrames
        - nan_threshold: Percentage threshold (0-1) for column removal (default 0.7)
        - drop_empty_rows: Whether to drop rows that become empty after column cleaning (default True)
        - fill_method: Strategy to fill remaining NaNs ('ffill', 'bfill', None) (default None)

        Returns:
        - List of merged DataFrames (clusters)
        """
        dataframes_list = self._dataframes if isinstance(self._dataframes, list) else [self._dataframes]

        if not dataframes_list:
            return pd.DataFrame()

        # Step 1: Cluster DataFrames by column similarity
        def column_similarity(df1, df2):
            """Calculate Jaccard similarity between DataFrame columns"""
            cols1 = set(df1.columns)
            cols2 = set(df2.columns)
            intersection = cols1.intersection(cols2)
            union = cols1.union(cols2)
            return len(intersection) / len(union) if union else 0

        # Create similarity matrix
        n = len(dataframes_list)
        similarity_matrix = np.zeros((n, n))
        for i in range(n):
            for j in range(i + 1, n):
                sim = column_similarity(dataframes_list[i], dataframes_list[j])
                similarity_matrix[i, j] = sim
                similarity_matrix[j, i] = sim

        # Cluster DataFrames
        clusters = []
        remaining_indices = set(range(n))

        while remaining_indices:
            # Start with the DataFrame that has highest average similarity
            seed_idx = max(remaining_indices,
                           key=lambda x: np.mean(similarity_matrix[x, list(remaining_indices)]))

            cluster = {seed_idx}
            remaining_indices.remove(seed_idx)

            # Find all similar DataFrames
            added = True
            while added:
                added = False
                for idx in list(remaining_indices):
                    if similarity_matrix[seed_idx, idx] >= similarity_threshold:
                        cluster.add(idx)
                        remaining_indices.remove(idx)
                        added = True

            clusters.append(cluster)

        # Step 2: Merge DataFrames within each cluster
        merged_dfs = []
        for cluster in clusters:
            cluster_dfs = [dataframes_list[i] for i in cluster]

            # Get all unique columns in this cluster
            cluster_columns = set()
            for df in cluster_dfs:
                cluster_columns.update(df.columns)

            cluster_columns = sorted(cluster_columns) if sort_columns else list(cluster_columns)

            # Reindex and merge
            reindexed_dfs = []
            for df in cluster_dfs:
                reindexed_df = df.reindex(columns=cluster_columns, fill_value=fill_value)
                reindexed_dfs.append(reindexed_df)

            merged_df = pd.concat(reindexed_dfs, axis=0, ignore_index=True)
            merged_df = clean_nan_columns(merged_df, nan_threshold=nan_threshold, drop_empty_rows=drop_empty_rows,
                                          fill_value=fill_value, fill_method=fill_method)
            merged_dfs.append(merged_df)

        # If only one cluster, return the single DataFrame directly
        if len(merged_dfs) == 1:
            return merged_dfs[0]

        return merged_dfs

def clean_nan_columns(df,
                      nan_threshold=1.0,
                      drop_empty_rows=False,
                      fill_method=None,
                      fill_value=None):
    """
    Clean DataFrame by removing columns with high NaN percentages.

    Parameters:
    - df: Input pandas DataFrame
    - nan_threshold: Percentage threshold (0-1) for column removal (default 0.7)
    - drop_empty_rows: Whether to drop rows that become empty after column cleaning (default False)
    - fill_method: Strategy to fill remaining NaNs ('ffill', 'bfill', None) (default None)
    - fill_value: Value to use for filling NaNs when fill_method is None (default None)

    Returns:
    - Cleaned pandas DataFrame
    """

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")

    nan_percent = df.isna().mean()
    cols_to_drop = nan_percent[nan_percent >= nan_threshold].index.tolist()
    cleaned_df = df.drop(columns=cols_to_drop)
    if drop_empty_rows:
        cleaned_df = cleaned_df.dropna(how='all')
    if fill_method in ['ffill', 'bfill']:
        cleaned_df = cleaned_df.fillna(method=fill_method)
    elif fill_value is not None:
        cleaned_df = cleaned_df.fillna(fill_value)

    if cols_to_drop:
        logging.info(f"Removed {len(cols_to_drop)} columns with ≥{nan_threshold * 100:.0f}% NaN values:")
        logging.info(cols_to_drop)
    else:
        logging.info("No columns met the NaN threshold for removal")

    return cleaned_df