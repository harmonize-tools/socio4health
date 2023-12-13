import os
import sqlite3
from pathlib import Path

from nyctibius.dto.data_info import DataInfo
from nyctibius.enums.config_enum import ConfigEnum


def get_column_datatypes(cnx, table_name):
    cursor = cnx.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [col[2] for col in cursor.fetchall()]


class Querier:
    """Data Loader class"""

    def __init__(self):
        directory = os.path.dirname(ConfigEnum.DB_PATH.value)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        self.db_path = ConfigEnum.DB_PATH.value

    def get_tables(self):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()
                cursor.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                return cursor.fetchall()
        except Exception as e:
            return False, f"Error getting tables: {str(e)}"

    def get_columns(self, table_name):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()
                cursor.execute(f'PRAGMA table_info({table_name})')
                columns = cursor.fetchall()
                return [column[1] for column in columns]
        except Exception as e:
            return False, f"Error getting tables: {str(e)}"

    def rename_table(self, initial_name, final_name):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the initial table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{initial_name}'")
                if not cursor.fetchone():
                    return False, f"Table '{initial_name}' not found"

                # Rename the table
                cursor.execute(f"ALTER TABLE {initial_name} RENAME TO {final_name}")

                return True, f"Table '{initial_name}' renamed to '{final_name}'"
        except Exception as e:
            return False, f"Error renaming table: {str(e)}"

    def rename_column(self, table_name, initial_name, final_name):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table and initial column exist
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                if initial_name not in columns:
                    return False, f"Column '{initial_name}' not found in table '{table_name}'"

                # Rename the column
                cursor.execute(f"PRAGMA foreign_keys=off")
                cursor.execute(f"BEGIN TRANSACTION")
                cursor.execute(f"CREATE TEMPORARY TABLE {table_name}_backup AS SELECT * FROM {table_name}")
                cursor.execute(f"DROP TABLE {table_name}")
                new_columns = [col if col != initial_name else final_name for col in columns]
                cursor.execute(f"CREATE TABLE {table_name} ({', '.join(new_columns)})")
                cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_backup")
                cursor.execute(f"DROP TABLE {table_name}_backup")
                cursor.execute(f"COMMIT")
                cursor.execute(f"PRAGMA foreign_keys=on")

                return True, f"Column '{initial_name}' in table '{table_name}' renamed to '{final_name}'"
        except Exception as e:
            return False, f"Error renaming column: {str(e)}"

    def rename_table_columns(self, table_name, final_names_list):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table exists
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                if not columns:
                    return False, f"Table '{table_name}' not found"

                # Check if the number of columns in final_names_list matches the existing columns
                if len(columns) != len(final_names_list):
                    return False, "Number of columns in final_names_list does not match the existing columns"

                # Create a mapping of old names to new names
                columns_mapping = {columns[i]: final_names_list[i] for i in range(len(columns))}

                # Rename the columns
                cursor.execute(f"PRAGMA foreign_keys=off")
                cursor.execute(f"BEGIN TRANSACTION")
                cursor.execute(f"CREATE TEMPORARY TABLE {table_name}_backup AS SELECT * FROM {table_name}")
                cursor.execute(f"DROP TABLE {table_name}")

                new_columns = [f"{columns_mapping[col]} {datatype}" for col, datatype in
                               zip(columns, get_column_datatypes(cnx, table_name))]
                cursor.execute(f"CREATE TABLE {table_name} ({', '.join(new_columns)})")

                cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_backup")
                cursor.execute(f"DROP TABLE {table_name}_backup")
                cursor.execute(f"COMMIT")
                cursor.execute(f"PRAGMA foreign_keys=on")

                return True, f"Columns in table '{table_name}' renamed successfully"
        except Exception as e:
            return False, f"Error renaming columns: {str(e)}"

    def set_primary_key(self, table_name, column_name):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table and column exist
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                if column_name not in columns:
                    return False, f"Column '{column_name}' not found in table '{table_name}'"

                # Check if the column is nullable
                cursor.execute(f"PRAGMA table_info({table_name})")
                column_info = cursor.fetchall()
                for col in column_info:
                    if col[1] == column_name and col[3] == 1:  # col[3] is the 'notnull' column
                        return False, f"Column '{column_name}' cannot be set as primary key because it is nullable"

                # Set the primary key
                cursor.execute(f"PRAGMA foreign_keys=off")
                cursor.execute(f"BEGIN TRANSACTION")
                cursor.execute(f"CREATE TEMPORARY TABLE {table_name}_backup AS SELECT * FROM {table_name}")
                cursor.execute(f"DROP TABLE {table_name}")
                cursor.execute(
                    f"CREATE TABLE {table_name} ({column_name} INTEGER PRIMARY KEY, UNIQUE ({column_name}), UNIQUE ({', '.join(columns)}))")
                cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_backup")
                cursor.execute(f"DROP TABLE {table_name}_backup")
                cursor.execute(f"COMMIT")
                cursor.execute(f"PRAGMA foreign_keys=on")

                return True, f"Primary key set successfully for column '{column_name}' in table '{table_name}'"
        except Exception as e:
            return False, f"Error setting primary key: {str(e)}"

    def set_column_not_nullable(self, table_name, column_name, is_nullable):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the table and column exist
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                if column_name not in columns:
                    return False, f"Column '{column_name}' not found in table '{table_name}'"

                # Update the column's nullable status
                cursor.execute(f"PRAGMA foreign_keys=off")
                cursor.execute(f"BEGIN TRANSACTION")
                cursor.execute(f"CREATE TEMPORARY TABLE {table_name}_backup AS SELECT * FROM {table_name}")
                cursor.execute(f"DROP TABLE {table_name}")
                nullable_clause = "" if is_nullable else "NOT NULL"
                cursor.execute(
                    f"CREATE TABLE {table_name} ({', '.join(columns).replace(column_name, f'{column_name} {nullable_clause}').replace(', ,', ',')})")
                cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_backup")
                cursor.execute(f"DROP TABLE {table_name}_backup")
                cursor.execute(f"COMMIT")
                cursor.execute(f"PRAGMA foreign_keys=on")

                return True, f"Column '{column_name}' set as {'nullable' if is_nullable else 'not nullable'} " \
                             f"in table '{table_name}'"
        except Exception as e:
            return False, f"Error setting column as {'nullable' if is_nullable else 'not nullable'}: {str(e)}"

    def set_foreign_key(self, column_name, main_table, secondary_table):
        try:
            with sqlite3.connect(self.db_path) as cnx:
                cursor = cnx.cursor()

                # Check if the main and secondary tables exist
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{main_table}'")
                if not cursor.fetchone():
                    return False, f"Main table '{main_table}' not found"

                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{secondary_table}'")
                if not cursor.fetchone():
                    return False, f"Secondary table '{secondary_table}' not found"

                # Check if the specified column exists in the main table
                cursor.execute(f"PRAGMA table_info({main_table})")
                columns = [col[1] for col in cursor.fetchall()]
                if column_name not in columns:
                    return False, f"Column '{column_name}' not found in main table '{main_table}'"

                # Set foreign key constraint
                cursor.execute(f"PRAGMA foreign_keys=on")
                cursor.execute(f"PRAGMA foreign_key_list({main_table})")
                existing_foreign_keys = cursor.fetchall()

                for fk in existing_foreign_keys:
                    if fk[3] == column_name and fk[2] == secondary_table:
                        return False, f"Foreign key constraint already exists for column '{column_name}' " \
                                      f"in table '{main_table}'"

                cursor.execute(
                    f"ALTER TABLE {main_table} ADD FOREIGN KEY ({column_name}) REFERENCES {secondary_table}({column_name})")

                return True, f"Foreign key constraint set for column '{column_name}' in table '{main_table}' " \
                             f"referencing table '{secondary_table}' "
        except Exception as e:
            return False, f"Error setting foreign key: {str(e)}"
