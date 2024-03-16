import os
import sqlite3
from pathlib import Path

from ..dto.data_info import DataInfo
from ..enums.config_enum import ConfigEnum


class Loader:
    """
    A class used to load data into a SQLite database.

    Attributes:
        db_path (str): The path to the SQLite database file.
    """

    def __init__(self, db_path=None):
        """
        Initialize the Loader with the path to the SQLite database file.

        If the db_path is not provided, the default path from ConfigEnum.DB_PATH.value will be used.

        Args:
            db_path (str, optional): The path to the SQLite database file. Defaults to ConfigEnum.DB_PATH.value.
        """
        if db_path is None:
            db_path = ConfigEnum.DB_PATH.value

        directory = os.path.dirname(db_path)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        self.db_path = db_path

    def load_data(self, dataInfo: DataInfo) -> tuple:
        """
        Load DataFrame into SQLite database.

        Args:
            dataInfo (DataInfo): The data information to load into the database.

        Returns:
            tuple: A tuple containing a boolean indicating the success of the operation and a message.
        """
        try:
            with sqlite3.connect(self.db_path) as cnx:
                # Set pragma settings
                cnx.execute('PRAGMA journal_mode = WAL')
                cnx.execute('PRAGMA synchronous = OFF')
                cnx.execute('PRAGMA temp_store = MEMORY')
                cnx.execute('PRAGMA mmap_size = 30000000000')

                # Create the table
                name = Path(dataInfo.file_path).stem
                dataInfo.data.to_sql(name, cnx, if_exists='append', chunksize=1000)

                # Optimize the database
                cnx.execute('PRAGMA optimize')

            return True, "Data loaded successfully"
        except Exception as e:
            return False, f"Error loading data: {str(e)}"
