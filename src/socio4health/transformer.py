import logging
import numpy as np
import pandas as pd
from typing import List, Union, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Transformer:
    def __init__(self, dictionary: pd.DataFrame = None,
                 dataframes: Optional[Union[List[pd.DataFrame], pd.DataFrame]] = None,
                 chunk_size: int = 9000):
        self._dictionary = dictionary
        self._dataframes = self._ensure_list(dataframes)
        self._chunk_size = chunk_size
        self._translated_dataframes = None

    def _ensure_list(self, dataframes) -> List[pd.DataFrame]:
        """Ensure input is always a list of DataFrames"""
        if dataframes is None:
            return []
        return dataframes if isinstance(dataframes, list) else [dataframes]

    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reduce memory usage by optimizing data types with chunked processing"""
        for col in df.columns:
            col_type = str(df[col].dtype)

            try:
                if col_type == 'object':
                    # Try converting to category first, then to string if it fails
                    try:
                        df[col] = df[col].astype('category')
                    except (ValueError, TypeError):
                        df[col] = df[col].astype('string')

                elif col_type in ['int64', 'float64']:
                    # Process numeric columns in chunks
                    if len(df) > self._chunk_size:
                        chunks = []
                        for i in range(0, len(df), self._chunk_size):
                            chunk = df[col].iloc[i:i + self._chunk_size]
                            if col_type == 'int64':
                                chunk = pd.to_numeric(chunk, downcast='integer')
                            else:
                                chunk = pd.to_numeric(chunk, downcast='float')
                            chunks.append(chunk)
                        df[col] = pd.concat(chunks)
                    else:
                        if col_type == 'int64':
                            df[col] = pd.to_numeric(df[col], downcast='integer')
                        else:
                            df[col] = pd.to_numeric(df[col], downcast='float')

            except MemoryError:
                logging.warning(f"Memory error optimizing column {col}. Keeping original dtype.")
                continue

        return df

    def _safe_concat(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Memory-safe concatenation of DataFrames"""
        if not dfs:
            return pd.DataFrame()

        # Exclude empty or all-NA DataFrames
        dfs = [df for df in dfs if not df.empty and not df.isna().all().all()]

        if not dfs:
            return pd.DataFrame()

        result = dfs[0]
        for df in dfs[1:]:
            if not df.empty and not df.isna().all().all():
                result = pd.concat([result, df], ignore_index=True)
                # Explicit memory management
                if len(result) > self._chunk_size:
                    result = self._optimize_dataframe(result)
        return result

    def _safe_dropna(self, df: pd.DataFrame, how: str = 'all') -> pd.DataFrame:
        """Process dropna in chunks"""
        if len(df) <= self._chunk_size:
            return df.dropna(how=how)

        chunks = []
        for i in range(0, len(df), self._chunk_size):
            chunk = df.iloc[i:i + self._chunk_size].copy()
            chunk = chunk.dropna(how=how)
            chunks.append(chunk)
        return self._safe_concat(chunks)

    def clean_nan_columns(self, df: pd.DataFrame, nan_threshold: float = 0.9,
                          drop_empty_rows: bool = False) -> pd.DataFrame:
        """Memory-optimized column cleaning"""
        try:
            # First optimize the input DataFrame
            df = self._optimize_dataframe(df)

            # Remove high-NaN columns
            nan_percent = df.isna().mean()
            cols_to_drop = nan_percent[nan_percent >= nan_threshold].index.tolist()
            cleaned_df = df.drop(columns=cols_to_drop)

            if drop_empty_rows:
                cleaned_df = self._safe_dropna(cleaned_df, how='all')

            return cleaned_df

        except MemoryError:
            # If we run out of memory, try with smaller chunks
            if self._chunk_size > 1000:
                logging.warning(f"Reducing chunk size from {self._chunk_size} to {self._chunk_size // 2}")
                original_size = self._chunk_size
                self._chunk_size = self._chunk_size // 2
                result = self.clean_nan_columns(df, nan_threshold, drop_empty_rows)
                self._chunk_size = original_size
                return result
            raise

    def vertical_merge(self, fill_value=None, sort_columns=False,
                       similarity_threshold=0.9, nan_threshold=0.9,
                       drop_empty_rows=True, fill_method=None) -> List[pd.DataFrame]:
        """Memory-safe vertical merge with clustering"""
        dataframes_list = [self._optimize_dataframe(df) for df in self._dataframes]

        if not dataframes_list:
            return []

        # Cluster DataFrames by column similarity
        def column_similarity(df1, df2):
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
            seed_idx = max(remaining_indices,
                           key=lambda x: np.mean(similarity_matrix[x, list(remaining_indices)]))
            cluster = {seed_idx}
            remaining_indices.remove(seed_idx)

            added = True
            while added:
                added = False
                for idx in list(remaining_indices):
                    if similarity_matrix[seed_idx, idx] >= similarity_threshold:
                        cluster.add(idx)
                        remaining_indices.remove(idx)
                        added = True
            clusters.append(cluster)

        # Process each cluster
        merged_dfs = []
        for cluster in clusters:
            cluster_dfs = [dataframes_list[i] for i in cluster]

            # Get all unique columns
            cluster_columns = set().union(*[df.columns for df in cluster_dfs])
            if sort_columns:
                cluster_columns = sorted(cluster_columns)

            # Process each DataFrame in chunks
            reindexed_chunks = []
            for df in cluster_dfs:
                if len(df) > self._chunk_size:
                    chunks = []
                    for i in range(0, len(df), self._chunk_size):
                        chunk = df.iloc[i:i + self._chunk_size]
                        chunk = chunk.reindex(columns=cluster_columns, fill_value=fill_value)
                        chunks.append(chunk)
                    reindexed_df = self._safe_concat(chunks)
                else:
                    reindexed_df = df.reindex(columns=cluster_columns, fill_value=fill_value)
                reindexed_chunks.append(reindexed_df)

            # Merge with memory safety
            merged_df = self._safe_concat(reindexed_chunks)
            merged_df = self.clean_nan_columns(
                merged_df,
                nan_threshold=nan_threshold,
                drop_empty_rows=drop_empty_rows
            )

            if fill_method in ['ffill', 'bfill']:
                merged_df = merged_df.fillna(method=fill_method)
            elif fill_value is not None:
                merged_df = merged_df.fillna(fill_value)

            merged_dfs.append(merged_df)

        return merged_dfs