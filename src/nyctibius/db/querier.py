import os

import pandas as pd

import sqlite3

from nyctibius.enums.config_enum import ConfigEnum


class Querier:
    """
    A helper class for executing SQL queries.

    Attributes:
        db_path (str): The path to the SQLite database file.
    """

    def __init__(self, db_path=None):
        """
        Initialize the Querier with the path to the SQLite database file.

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

    def _execute_query(self, query, params=()):
        """
        Execute a SQL query and return the result as a DataFrame.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters to substitute into the query.

        Returns:
            pandas.DataFrame: The result of the query.
        """
        with sqlite3.connect(self.db_path) as cnx:
            return pd.read_sql_query(query, cnx, params=params)

    def select(self, table, columns='*', where=None, limit=None):
        """
        Execute a SELECT query.

        Args:
            table (str): The name of the table to select from.
            columns (str or list, optional): The columns to select. Defaults to '*'.
            where (str, optional): The WHERE clause of the query.
            limit (int, optional): The maximum number of rows to return.

        Returns:
            pandas.DataFrame: The result of the query.
        """
        if isinstance(columns, list):
            columns = ', '.join(columns)

        query = f'SELECT {columns} FROM "{table}"'
        if where:
            query += f" WHERE {where}"
        if limit is not None:
            query += f" LIMIT {limit}"
        return self._execute_query(query)

    def insert(self, table, columns, values):
        """
        Execute an INSERT query.

        Args:
            table (str): The name of the table to insert into.
            columns (str or list, optional): The columns to insert values into.
            values (str): The values to insert.

        Returns:
            pandas.DataFrame: The result of the query.
        """
        if isinstance(columns, list):
            columns = ', '.join(columns)

        query = f'INSERT INTO "{table}" ({columns}) VALUES ({values})'
        return self._execute_query(query)

    def update(self, table, set_clause, where=None):
        """
        Execute an UPDATE query.

        Args:
            table (str): The name of the table to update.
            set_clause (str): The SET clause of the query.
            where (str, optional): The WHERE clause of the query.

        Returns:
            pandas.DataFrame: The result of the query.
        """
        query = f'UPDATE "{table}" SET {set_clause}'
        if where:
            query += f" WHERE {where}"
        return self._execute_query(query)

    def delete(self, table, where=None):
        """
        Execute a DELETE query.

        Args:
            table (str): The name of the table to delete from.
            where (str, optional): The WHERE clause of the query.

        Returns:
            pandas.DataFrame: The result of the query.
        """
        query = f'DELETE FROM "{table}"'
        if where:
            query += f" WHERE {where}"
        return self._execute_query(query)

    def join(self, table1, table2, columns='*', join_type='INNER', on_condition=None):
        """
        Execute a JOIN query.

        Args:
            table1 (str): The name of the first table to join.
            table2 (str): The name of the second table to join.
            columns (str or list, optional): The columns to select. Defaults to '*'.
            join_type (str, optional): The type of join to perform. Defaults to 'INNER'.
            on_condition (str, optional): The condition for the join.

        Returns:
            pandas.DataFrame: The result of the query.
        """

        if isinstance(columns, list):
            columns = ', '.join(columns)

        if on_condition is None:
            raise ValueError("The 'on_condition' argument must be provided for a join operation.")

        query = f'SELECT {columns} FROM "{table1}" {join_type} JOIN "{table2}" ON {on_condition}'
        return self._execute_query(query)
