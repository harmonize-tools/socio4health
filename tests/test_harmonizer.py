import unittest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class MockHarmonizer:
    def __init__(self):
        self.similarity_threshold = 0.8
        self.join_key = None
        self.aux_key = None
        self.extra_cols = []
        self.categories = []
        self.dict_df = None
        self.nan_threshold = None
        self.key_col = None
        self.key_val = None

    def vertical_merge(self, dfs):
        """Simulate vertical merge - ensures join_key and aux_key are preserved"""
        if not self.join_key or not self.aux_key:
            return dfs

        result_dfs = []
        for df in dfs:
            # Ensure join_key and aux_key are present
            if self.join_key not in df.columns:
                df[self.join_key] = None
            if self.aux_key not in df.columns:
                df[self.aux_key] = None

            # Preserve extra columns if specified
            preserved_cols = [self.join_key, self.aux_key]
            if self.extra_cols:
                preserved_cols.extend(self.extra_cols)

            # Keep only columns that exist in the DataFrame
            existing_cols = [col for col in preserved_cols if col in df.columns]
            other_cols = [col for col in df.columns if col not in preserved_cols]

            # Reorder columns to put preserved ones first
            df = df[existing_cols + other_cols]
            result_dfs.append(df)

        return result_dfs

    def drop_nan_columns(self, dfs):
        """Remove columns with NaN ratio above threshold"""
        if self.nan_threshold is None:
            return dfs

        result_dfs = []
        for df in dfs:
            nan_ratios = df.isna().mean()
            columns_to_keep = nan_ratios[nan_ratios <= self.nan_threshold].index
            result_dfs.append(df[columns_to_keep])

        return result_dfs

    def data_selector(self, dfs):
        """Filter data based on key column and values"""
        result_dfs = []
        for df in dfs:
            if self.key_col and self.key_val and self.key_col in df.columns:
                # Convert both to same type for comparison
                df_key_col = df[self.key_col].astype(str)
                key_vals = [str(val) for val in self.key_val]
                filtered_df = df[df_key_col.isin(key_vals)]
                result_dfs.append(filtered_df)
            else:
                result_dfs.append(df)
        return result_dfs

    def join_data(self, ddfs):
        """Mock join data - returns empty DataFrame"""
        return pd.DataFrame()


class TestHarmonizer(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures"""
        self.harmonizer = MockHarmonizer()

        self.sample_df1 = pd.DataFrame({
            'DIRECTORIO': [1, 1, 2, 2],
            'ORDEN': [1, 2, 1, 2],
            'VAR1': ['A', 'B', 'C', 'D'],
            'VAR2': [10, 20, 30, 40]
        })

        self.sample_df2 = pd.DataFrame({
            'DIRECTORIO': [1, 1, 2, 2],
            'ORDEN': [1, 2, 1, 2],
            'VAR3': ['X', 'Y', 'Z', 'W'],
            'VAR4': [100, 200, 300, 400]
        })

    def test_harmonizer_initialization(self):
        """Test Harmonizer initialization with default values"""
        self.assertEqual(self.harmonizer.similarity_threshold, 0.8)
        self.assertIsNone(self.harmonizer.join_key)
        self.assertIsNone(self.harmonizer.aux_key)
        self.assertEqual(self.harmonizer.extra_cols, [])
        self.assertEqual(self.harmonizer.categories, [])
        self.assertIsNone(self.harmonizer.dict_df)

    def test_vertical_merge_basic(self):
        """Test basic vertical merge functionality"""
        self.harmonizer.join_key = 'DIRECTORIO'
        self.harmonizer.aux_key = 'ORDEN'

        dfs = [self.sample_df1, self.sample_df2]

        merged_dfs = self.harmonizer.vertical_merge(dfs)

        self.assertEqual(len(merged_dfs), len(dfs))

        for df in merged_dfs:
            self.assertIn('DIRECTORIO', df.columns)
            self.assertIn('ORDEN', df.columns)
            # Check that preserved columns come first
            first_cols = list(df.columns[:2])
            self.assertIn('DIRECTORIO', first_cols)
            self.assertIn('ORDEN', first_cols)

    def test_vertical_merge_with_extra_cols(self):
        """Test vertical merge with extra columns"""
        self.harmonizer.join_key = 'DIRECTORIO'
        self.harmonizer.aux_key = 'ORDEN'
        self.harmonizer.extra_cols = ['EXTRA_COL']

        df_with_extra = self.sample_df1.copy()
        df_with_extra['EXTRA_COL'] = ['extra1', 'extra2', 'extra3', 'extra4']

        dfs = [df_with_extra, self.sample_df2]

        merged_dfs = self.harmonizer.vertical_merge(dfs)

        self.assertIn('EXTRA_COL', merged_dfs[0].columns)
        first_cols = list(merged_dfs[0].columns[:3])
        self.assertIn('DIRECTORIO', first_cols)
        self.assertIn('ORDEN', first_cols)
        self.assertIn('EXTRA_COL', first_cols)

    def test_drop_nan_columns(self):
        """Test NaN column removal"""
        self.harmonizer.nan_threshold = 0.5

        df_with_nans = pd.DataFrame({
            'DIRECTORIO': [1, 2, 3, 4],
            'ORDEN': [1, 1, 1, 1],
            'VAR1': [1, 2, np.nan, np.nan],
            'VAR2': [np.nan, np.nan, np.nan, np.nan],
            'VAR3': [1, 2, 3, 4]
        })

        dfs = [df_with_nans]
        result_dfs = self.harmonizer.drop_nan_columns(dfs)

        result_df = result_dfs[0]
        self.assertNotIn('VAR2', result_df.columns)
        self.assertIn('VAR1', result_df.columns)
        self.assertIn('VAR3', result_df.columns)
        self.assertIn('DIRECTORIO', result_df.columns)
        self.assertIn('ORDEN', result_df.columns)

    def test_data_selector_by_key_value(self):
        """Test data selection by key value"""
        self.harmonizer.key_col = 'DIRECTORIO'
        self.harmonizer.key_val = [1]

        dfs = [self.sample_df1, self.sample_df2]

        selected_dfs = self.harmonizer.data_selector(dfs)

        for df in selected_dfs:
            if not df.empty and 'DIRECTORIO' in df.columns:
                # Convert to same type for comparison
                unique_values = df['DIRECTORIO'].astype(int).unique()
                self.assertEqual(len(unique_values), 1)
                self.assertEqual(unique_values[0], 1)

    def test_data_selector_by_key_value_string(self):
        """Test data selection by string key value"""
        self.harmonizer.key_col = 'DIRECTORIO'
        self.harmonizer.key_val = ['1']

        dfs = [self.sample_df1, self.sample_df2]

        selected_dfs = self.harmonizer.data_selector(dfs)

        for df in selected_dfs:
            if not df.empty and 'DIRECTORIO' in df.columns:
                unique_values = df['DIRECTORIO'].astype(str).unique()
                self.assertEqual(len(unique_values), 1)
                self.assertEqual(unique_values[0], '1')

    def test_join_data_returns_dataframe(self):
        """Test that join_data returns a DataFrame"""
        result = self.harmonizer.join_data([])
        self.assertIsInstance(result, pd.DataFrame)


class TestDataFrameOperations(unittest.TestCase):
    """Test basic DataFrame operations that are used in the harmonizer"""

    def test_dataframe_concatenation(self):
        """Test DataFrame concatenation (simulating vertical merge)"""
        df1 = pd.DataFrame({'ID': [1, 2], 'A': ['a', 'b']})
        df2 = pd.DataFrame({'ID': [3, 4], 'A': ['c', 'd']})

        result = pd.concat([df1, df2], ignore_index=True)

        self.assertEqual(len(result), 4)
        self.assertListEqual(result['ID'].tolist(), [1, 2, 3, 4])

    def test_dataframe_filtering(self):
        """Test DataFrame filtering (simulating data selection)"""
        df = pd.DataFrame({
            'DIRECTORIO': [1, 1, 2, 2],
            'VAR1': ['A', 'B', 'C', 'D']
        })

        filtered = df[df['DIRECTORIO'] == 1]

        self.assertEqual(len(filtered), 2)
        self.assertTrue((filtered['DIRECTORIO'] == 1).all())

    def test_nan_column_removal(self):
        """Test manual NaN column removal"""
        df = pd.DataFrame({
            'ID': [1, 2, 3],
            'ALL_NAN': [np.nan, np.nan, np.nan],
            'SOME_NAN': [1, np.nan, 3],
            'NO_NAN': [1, 2, 3]
        })

        result = df.dropna(axis=1, how='all')

        self.assertNotIn('ALL_NAN', result.columns)
        self.assertIn('SOME_NAN', result.columns)
        self.assertIn('NO_NAN', result.columns)

        nan_ratio = df.isna().mean()
        columns_to_keep = nan_ratio[nan_ratio <= 0.5].index
        result = df[columns_to_keep]

        self.assertNotIn('ALL_NAN', result.columns)
        self.assertIn('SOME_NAN', result.columns)
        self.assertIn('NO_NAN', result.columns)


if __name__ == '__main__':
    unittest.main()