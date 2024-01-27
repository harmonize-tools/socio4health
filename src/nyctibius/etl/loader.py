import os
import sqlite3
from pathlib import Path

from ..dto.data_info import DataInfo
from ..enums.config_enum import ConfigEnum


class Loader:
    """Data Loader class"""

    def __init__(self):
        directory = os.path.dirname(ConfigEnum.DB_PATH.value)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        self.db_path = ConfigEnum.DB_PATH.value

    def load_data(self, dataInfo: DataInfo) -> tuple:
        """Load DataFrame into SQLite database"""
        try:
            with sqlite3.connect(self.db_path) as cnx:
                # Set pragma settings
                cnx.execute('PRAGMA journal_mode = WAL')
                cnx.execute('PRAGMA synchronous = OFF')
                cnx.execute('PRAGMA temp_store = MEMORY')
                cnx.execute('PRAGMA mmap_size = 30000000000')

                # Create the table
                name = Path(dataInfo.file_path).stem
                if dataInfo.name is not None:
                    name = dataInfo.name
                dataInfo.data.to_sql(name, cnx, if_exists='append', chunksize=1000)

                # Optimize the database
                cnx.execute('PRAGMA optimize')

            return True, "Data loaded successfully"
        except Exception as e:
            return False, f"Error loading data: {str(e)}"