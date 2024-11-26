import sqlite3
import json


def load_mapping(json_file):
    """
    Load the harmonized-to-local mapping from a JSON file.

    :param json_file: Path to the JSON file containing the mappings.
    :return: A dictionary with the mappings.
    """
    with open(json_file, 'r') as f:
        return json.load(f)


def query_harmonized(sqlite_file, json_file, data_source, harmonized_category, harmonized_value):
    """
    Query over a SQLite table using harmonized variable names and values.

    :param sqlite_file: Path to the SQLite file.
    :param json_file: Path to the JSON file containing the mappings.
    :param data_source: The data source to query (e.g., 'BRC2010', 'COC2018', etc.).
    :param harmonized_category: The harmonized category (e.g., 'area_class', 'dwelling_type').
    :param harmonized_value: The harmonized value to filter (e.g., '10' for Urban).
    :return: Query results as a list of tuples.
    """

    # Load the mapping from the JSON file
    harmonized_to_local_map = load_mapping(json_file)

    # Check if the harmonized category and data source exist in the mapping
    if harmonized_category not in harmonized_to_local_map or data_source not in \
            harmonized_to_local_map[harmonized_category]["data_sources"]:
        raise ValueError(
            f"No mapping found for data source {data_source} and harmonized category {harmonized_category}")

    # Get the data source variables and mappings
    data_source_info = harmonized_to_local_map[harmonized_category]["data_sources"][data_source]

    # Iterate over variables and check for mapping of harmonized value
    for local_var, var_info in data_source_info["variables"].items():
        for local_val, mapping_info in var_info["mappings"].items():
            if mapping_info["map_to"] == harmonized_value:
                # Construct the SQL query using the local variable and value
                conn = sqlite3.connect(sqlite_file)
                cursor = conn.cursor()
                query = f"SELECT * FROM {data_source} WHERE {local_var} = ?"
                cursor.execute(query, (local_val,))
                results = cursor.fetchall()
                conn.close()

                return results

    raise ValueError(f"No mapping found for harmonized value {harmonized_value} in data source {data_source}")


# Example usage

json_file = 'harmonized_to_local_map.json'  # Path to your JSON file
data_source = 'BRC2010'
harmonized_category = 'area_class'
harmonized_value = '10'  # Query for 'Urban'

results = query_harmonized(sqlite_file, json_file, data_source, harmonized_category, harmonized_value)
for row in results:
    print(row)
