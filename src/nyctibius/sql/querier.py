import os
import sqlite3
from nyctibius.enums.config_enum import ConfigEnum


class Querier:
    """Data Loader class"""

    def __init__(self):
        directory = os.path.dirname(ConfigEnum.DB_PATH.value)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        self.db_path = ConfigEnum.DB_PATH.value

    def get_column_datatypes(self, table_name: str):
        """
        Get the datatypes of all columns in a table.

        Args:
            table_name (str): The name of the table.

        Returns:
            A dictionary containing the status of the operation and the list of column datatypes or error message.
        """
        query = f"PRAGMA table_info({table_name});"
        result = self.execute_query(query)
        if result['status'] == 'success':
            # Extract column names and datatypes
            result['result'] = [(column[1], column[2]) for column in result['result']]
        return result

    def execute_query(self, query: str, params: tuple = ()):
        """
        Execute a SQL query.

        Args:
            query (str): The SQL query to execute.
            params (tuple): The parameters to substitute in the query.

        Returns:
            A dictionary containing the status of the operation and the result or error message.
        """
        result = {'status': None, 'result': None, 'error': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()
                cursor.execute(query, params)
                result['status'] = 'success'
                result['result'] = cursor.fetchall()
        except Exception as e:
            result['status'] = 'failure'
            result['error'] = str(e)

        return result

    def get_tables(self):
        """
        Get a list of all tables in the database.

        Returns:
            A dictionary containing the status of the operation and the list of tables or error message.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        return self.execute_query(query)

    def get_columns(self, table_name: str):
        """
        Get a list of all columns in a table.

        Args:
            table_name (str): The name of the table.

        Returns:
            A dictionary containing the status of the operation and the list of columns or error message.
        """
        query = f"PRAGMA table_info({table_name});"
        return self.execute_query(query)
    
    def rename_table(self, old_table_name: str, new_table_name: str):
        """
        Rename a table in the database.

        Args:
            old_table_name (str): The current name of the table.
            new_table_name (str): The new name for the table.

        Returns:
            A dictionary containing the status of the operation and a message.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()
                cursor.execute(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}")
                result['status'] = 'success'
                result['message'] = f"Table '{old_table_name}' renamed to '{new_table_name}'"
        except Exception as e:
            result['status'] = 'failure'
            result['message'] = str(e)

        return result

    def rename_column(self, table_name: str, old_column_name: str, new_column_name: str):
        """
        Rename a column in a table.

        Args:
            table_name (str): The name of the table where the column is located.
            old_column_name (str): The current name of the column.
            new_column_name (str): The new name for the column.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table and column exist
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                if old_column_name not in columns:
                    result['status'] = False
                    result['message'] = f"Column '{old_column_name}' not found in table '{table_name}'"
                    return result

                # Rename the column
                cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name}")

                result['status'] = True
                result['message'] = f"Column '{old_column_name}' renamed to '{new_column_name}' in table '{table_name}'"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error renaming column: {str(e)}"

        return result

    def rename_table_columns(self, table_name: str, new_column_names: list):
        """
        Rename the columns of a table.

        Args:
            table_name (str): The name of the table where the columns are located.
            new_column_names (list): A list of new column names.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not cursor.fetchone():
                    result['status'] = False
                    result['message'] = f"Table '{table_name}' not found"
                    return result

                # Get the current column names
                cursor.execute(f"PRAGMA table_info({table_name})")
                current_column_names = [column[1] for column in cursor.fetchall()]

                # Check if the number of new column names matches the number of current column names
                if len(new_column_names) != len(current_column_names):
                    result['status'] = False
                    result[
                        'message'] = "The number of new column names does not match the number of current column names"
                    return result

                # Create a new table with the new column names and copy the data from the old table
                cursor.execute(f"CREATE TABLE temp_table ({', '.join(new_column_names)})")
                cursor.execute(f"INSERT INTO temp_table SELECT * FROM {table_name}")

                # Delete the old table and rename the new table
                cursor.execute(f"DROP TABLE {table_name}")
                cursor.execute(f"ALTER TABLE temp_table RENAME TO {table_name}")

                result['status'] = True
                result['message'] = f"Columns in table '{table_name}' renamed successfully"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error renaming columns: {str(e)}"

        return result

    def remove_duplicates(self, table_name: str, column_name: str):
        """
        Remove duplicate rows in a table based on a specific column.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column.

        Returns:
            A dictionary containing the status of the operation and a message.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()
                cursor.execute(
                    f"DELETE FROM {table_name} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {table_name} GROUP BY {column_name})")
                result['status'] = 'success'
                result['message'] = f"Duplicates based on column '{column_name}' removed from table '{table_name}'"
        except Exception as e:
            result['status'] = 'failure'
            result['message'] = str(e)

        return result

    def set_primary_key(self, table_name: str, pk_column_name: str):
        """
        Set a column as the primary key of a table.

        Args:
            table_name (str): The name of the table where the column is located.
            pk_column_name (str): The name of the column to set as the primary key.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not cursor.fetchone():
                    result['status'] = False
                    result['message'] = f"Table '{table_name}' not found"
                    return result

                # Get the current column names
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]

                # Check if the column exists
                if pk_column_name not in columns:
                    result['status'] = False
                    result['message'] = f"Column '{pk_column_name}' not found in table '{table_name}'"
                    return result

                # Get the foreign key information
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                foreign_keys = cursor.fetchall()

                # Create a new table with the same columns as the old table, but set the primary key
                columns.remove(pk_column_name)
                foreign_keys_sql = ', '.join(
                    [f"FOREIGN KEY ({fk[3]}) REFERENCES {fk[2]}({fk[4]})" for fk in foreign_keys])
                cursor.execute(
                    f"CREATE TABLE temp_table ({pk_column_name} PRIMARY KEY, {', '.join(columns)}, {foreign_keys_sql})")

                # Copy the data from the old table to the new one
                cursor.execute(f"INSERT INTO temp_table SELECT * FROM {table_name}")

                # Delete the old table and rename the new table
                cursor.execute(f"DROP TABLE {table_name}")
                cursor.execute(f"ALTER TABLE temp_table RENAME TO {table_name}")

                result['status'] = True
                result['message'] = f"Primary key set to column '{pk_column_name}' in table '{table_name}'"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error setting primary key: {str(e)}"

        return result

    def set_foreign_key(self, table_name: str, fk_column_name: str, referenced_table_name: str, referenced_column_name: str):
        """
        Set a column as a foreign key of a table.

        Args:
            table_name (str): The name of the table where the column is located.
            fk_column_name (str): The name of the column to set as the foreign key.
            referenced_table_name (str): The name of the table that the foreign key references.
            referenced_column_name (str): The name of the column that the foreign key references.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not cursor.fetchone():
                    result['status'] = False
                    result['message'] = f"Table '{table_name}' not found"
                    return result

                # Get the current column names
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]

                # Check if the column exists
                if fk_column_name not in columns:
                    result['status'] = False
                    result['message'] = f"Column '{fk_column_name}' not found in table '{table_name}'"
                    return result

                # Get the primary key information
                cursor.execute(f"PRAGMA table_info({table_name})")
                primary_key_column = next((column[1] for column in cursor.fetchall() if column[5] == 1), None)

                # Create a new table with the same columns as the old table, but set the foreign key
                columns.remove(fk_column_name)
                primary_key_sql = f"{primary_key_column} PRIMARY KEY," if primary_key_column else ""
                cursor.execute(
                    f"CREATE TABLE temp_table ({primary_key_sql} {fk_column_name}, {', '.join(columns)}, FOREIGN KEY ({fk_column_name}) REFERENCES {referenced_table_name}({referenced_column_name}))")

                # Copy the data from the old table to the new one
                cursor.execute(f"INSERT INTO temp_table SELECT * FROM {table_name}")

                # Delete the old table and rename the new table
                cursor.execute(f"DROP TABLE {table_name}")
                cursor.execute(f"ALTER TABLE temp_table RENAME TO {table_name}")

                result['status'] = True
                result['message'] = f"Foreign key set to column '{fk_column_name}' in table '{table_name}'"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error setting foreign key: {str(e)}"

        return result

    def delete_table(self, table_name: str):
        """
        Delete a table from the database.

        Args:
            table_name (str): The name of the table to delete.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not cursor.fetchone():
                    result['status'] = False
                    result['message'] = f"Table '{table_name}' not found"
                    return result

                # Delete the table
                cursor.execute(f"DROP TABLE {table_name}")

                result['status'] = True
                result['message'] = f"Table '{table_name}' deleted successfully"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error deleting table: {str(e)}"

        return result

    def delete_column(self, table_name: str, column_name: str):
        """
        Delete a column from a table.

        Args:
            table_name (str): The name of the table where the column is located.
            column_name (str): The name of the column to delete.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table and column exist
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                if column_name not in columns:
                    result['status'] = False
                    result['message'] = f"Column '{column_name}' not found in table '{table_name}'"
                    return result

                # Delete the column
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")

                result['status'] = True
                result['message'] = f"Column '{column_name}' deleted from table '{table_name}'"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error deleting column: {str(e)}"

        return result

    def merge_tables(self, table1: str, table2: str, new_table: str):
        """
        Merge two tables into a new table.

        Args:
            table1 (str): The name of the first table to merge.
            table2 (str): The name of the second table to merge.
            new_table (str): The name of the new table.

        Returns:
            A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not.
            'message' is a string describing the result of the operation.
        """
        result = {'status': None, 'message': None}
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the tables exist
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table1}'")
                if not cursor.fetchone():
                    result['status'] = False
                    result['message'] = f"Table '{table1}' not found"
                    return result

                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table2}'")
                if not cursor.fetchone():
                    result['status'] = False
                    result['message'] = f"Table '{table2}' not found"
                    return result

                # Merge the tables
                cursor.execute(f"CREATE TABLE {new_table} AS SELECT * FROM {table1} UNION ALL SELECT * FROM {table2}")

                result['status'] = True
                result['message'] = f"Tables '{table1}' and '{table2}' merged into new table '{new_table}'"
        except Exception as e:
            result['status'] = False
            result['message'] = f"Error merging tables: {str(e)}"

        return result