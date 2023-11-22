import sqlite3

from harmonize.dto.dataset import Dataset
from harmonize.enum.config_enum import ConfigEnum


class Loader:
    """Data Loader class"""

    def __init__(self):
        self.cnx = sqlite3.connect(ConfigEnum.DB_PATH.value)

    def load_data(self, dataset: Dataset) -> tuple:
        """Load DataFrame into SQLite database"""
        try:
            dataset.data.to_sql(dataset.name, self.cnx, if_exists='append', index=False)
            self.cnx.commit()
            self.cnx.close()
            return True, "Data loaded successfully"
        except Exception as e:
            self.cnx.close()
            return False, f"Error loading data: {str(e)}"
