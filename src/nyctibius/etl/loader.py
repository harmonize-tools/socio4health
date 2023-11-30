import os
import sqlite3
from nyctibius.dto.data_info import DataInfo
from nyctibius.enums.config_enum import ConfigEnum


class Loader:
    """Data Loader class"""

    def __init__(self):
        directory = os.path.dirname(ConfigEnum.DB_PATH.value)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        self.db_path = ConfigEnum.DB_PATH.value

    def load_data(self, dataset: DataInfo) -> tuple:
        """Load DataFrame into SQLite database"""
        try:
            with sqlite3.connect(self.db_path) as cnx:
                dataset.data.to_sql(dataset.file_path, cnx, if_exists='append', index=False, chunksize=1000)
            return True, "Data loaded successfully"
        except Exception as e:
            return False, f"Error loading data: {str(e)}"
