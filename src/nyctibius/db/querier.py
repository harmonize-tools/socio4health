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
        self.query = ''

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

    def select(self, table, columns=None):
        """
        Start a SELECT query for a specific table and columns.

        Args:
            table (str): The name of the table to select from.
            columns (list, optional): The columns to select. Defaults to ['*'].

        Returns:
            Querier: The current Querier instance.
        """
        if columns is None:
            columns = ['*']
        if isinstance(columns, list):
            columns = ', '.join(columns)
        self.query = f'SELECT {columns} FROM "{table}"'
        return self

    def filter(self, condition, operator='AND'):
        """
        Add a condition to the query.

        Args:
            condition (str): The condition for the clause.
            operator (str, optional): The operator for the clause. Defaults to 'AND'.

        Returns:
            Querier: The current Querier instance.
        """
        if operator not in ['AND', 'OR']:
            raise ValueError("Operator must be either 'AND' or 'OR'")

        if 'WHERE' in self.query:
            self.query += f' {operator} {condition}'
        else:
            self.query += f' WHERE {condition}'
        return self

    def filter_in(self, column, values, operator='AND', not_in=False):
        """
        Add an IN or NOT IN condition to the query.

        Args:
            column (str): The column for the IN condition.
            values (list): The values for the IN condition.
            operator (str, optional): The operator for the clause. Defaults to 'AND'.
            not_in (bool, optional): Whether it's a NOT IN condition. Defaults to False.

        Returns:
            Querier: The current Querier instance.
        """
        if operator not in ['AND', 'OR']:
            raise ValueError("Operator must be either 'AND' or 'OR'")

        values = ', '.join(map(str, values))
        in_clause = 'NOT IN' if not_in else 'IN'

        if 'WHERE' in self.query:
            self.query += f' {operator} {column} {in_clause} ({values})'
        else:
            self.query += f' WHERE {column} {in_clause} ({values})'
        return self

    def filter_like(self, column, pattern, condition_type, operator='AND'):
        """
        Add a LIKE condition to the query that checks if a column starts with, ends with, or contains a pattern.

        Args:
            column (str): The column for the LIKE condition.
            pattern (str): The pattern for the LIKE condition.
            condition_type (str): The type of LIKE condition. Must be 'startswith', 'endswith', or 'contains'.
            operator (str, optional): The operator for the clause. Defaults to 'AND'.

        Returns:
            Querier: The current Querier instance.
        """
        if operator not in ['AND', 'OR']:
            raise ValueError("Operator must be either 'AND' or 'OR'")
        if condition_type not in ['startswith', 'endswith', 'contains']:
            raise ValueError("Condition type must be either 'startswith', 'endswith', or 'contains'")

        if condition_type == 'startswith':
            pattern = f'{pattern}%'
        elif condition_type == 'endswith':
            pattern = f'%{pattern}'
        else:  # condition_type == 'contains'
            pattern = f'%{pattern}%'

        if 'WHERE' in self.query:
            self.query += f' {operator} {column} LIKE "{pattern}"'
        else:
            self.query += f' WHERE {column} LIKE "{pattern}"'
        return self

    def join(self, table, join_type='INNER', on_condition=None):
        """
        Add a JOIN clause to the query.

        Args:
            table (str): The table to join with.
            join_type (str, optional): The type of join. Defaults to 'INNER'.
            on_condition (str, optional): The condition for the ON clause. Defaults to None.

        Returns:
            Querier: The current Querier instance.
        """
        self.query += f' {join_type} JOIN "{table}"'
        if on_condition is not None:
            self.query += f' ON {on_condition}'
        return self

    def limit(self, limit):
        """
        Add a LIMIT clause to the query.

        Args:
            limit (int): The maximum number of rows to return.

        Returns:
            Querier: The current Querier instance.
        """
        self.query += f' LIMIT {limit}'
        return self

    def execute(self):
        """
        Execute the current query and return the result as a DataFrame.

        Returns:
            pandas.DataFrame: The result of the query.
        """
        return self._execute_query(self.query)
