import unittest
import pandas as pd
import os

from harmonize.etl.transformer import Transformer


class TestTransformer(unittest.TestCase):
    """
    Unit tests for the Transformer class
    """

    def setUp(self):
        """
        Set up test data and objects
        """
        # Create a test CSV file
        self.test_file = 'test_file.csv'
        self.data = {
            'old_col1': [1, 2, 3],
            'old_col2': ['a', 'b', 'c']
        }
        df = pd.DataFrame(self.data)
        df.to_csv(self.test_file, index=False)

        # Create a Transformer object
        self.transformer = Transformer(
            self.test_file,
            ['old_col1', 'old_col2'],
            ['new_col1', 'new_col2'],
            'test_database.db',
            'test_table'
        )

    def tearDown(self):
        """
        Clean up test data and objects
        """
        # Remove the test CSV file
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

        # Remove the SQLite database file
        if os.path.exists('test_database.db'):
            os.remove('test_database.db')

    def test_transform_data(self):
        """
        Test the transform_data method
        """
        # Transform the data
        df = self.transformer.transform_data()

        # Check that the DataFrame has the correct data
        self.assertEqual(list(df.columns), ['new_col1', 'new_col2'])
        self.assertEqual(list(df['new_col1']), [1, 2, 3])
        self.assertEqual(list(df['new_col2']), ['a', 'b', 'c'])


if __name__ == '__main__':
    unittest.main()
