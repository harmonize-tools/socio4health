import os
import duckdb


class Translator:
    def __init__(self, mapping_path=None, data_info=None):
        # Set the default mapping path to the directory of the Translator file
        if mapping_path is None:
            # Get the directory where this file is located
            dir_path = os.path.dirname(os.path.abspath(__file__))
            mapping_path = os.path.join(dir_path, 'MAPPING.csv')
        self.data_info = data_info

        # Load the CSV file into DuckDB's in-memory table
        self.conn = duckdb.connect()
        self.conn.execute(f"CREATE TABLE mapping AS SELECT * FROM read_csv_auto('{mapping_path}')")

    def query(self, query, params=None):
        # Execute a query with optional parameters
        if params:
            return self.conn.execute(query, params).fetchall()
        return self.conn.execute(query).fetchall()

    def mapped_to_variable(self, variable):
        """
        Returns the original variable name based on the provided harmonized variable name.

        Args:
        variable (str): The harmonized variable name (original variable name from the dataset).

        Returns:
        str: The original variable name or None if not found.
        """
        query = """
        SELECT "Variable" 
        FROM mapping 
        WHERE "Mapped Variable" = ? AND "Country" = ? AND "Year" = ? AND "Data Source Type" = ?
        LIMIT 1
        """

        result = self.query(query, (variable, self.data_info.country, self.data_info.year, self.data_info.data_source_type))

        if result:
            return result[0][0]
        return None

    def variable_to_mapped(self, variable):
        """
        Returns the harmonized variable name based on the provided non-harmonized variable name.
        If the "Mapped Variable" is None, it searches for the next valid (non-None) mapping.

        Args:
        variable (str): The non-harmonized variable name (original variable name from the dataset).

        Returns:
        str: The harmonized variable name or None if not found.
        """
        query = """
        SELECT "Mapped Variable", "Country", "Year", "Data Source Type"
        FROM mapping 
        WHERE "Variable" = ? AND "Mapped Variable" IS NOT NULL
        LIMIT 1
        """

        result = self.query(query, (variable,))

        if result:
            if self.data_info.country is None:
                self.data_info.country = result[0][1]
            if self.data_info.year is None:
                self.data_info.year = int(result[0][2])
            if self.data_info.data_source_type is None:
                self.data_info.data_source_type = result[0][3]
            return result[0][0]
        return None
