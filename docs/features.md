---
title: Features
layout: page
nav_order: 4
---

# Features

## Extraction

Extracts data from either a web URL or a local directory path.

Parameters:
- url (str): The URL of the web page from which data needs to be extracted.
- path (str): The local directory path from which data needs to be extracted.
- depth (int): The depth of crawling if a URL is provided (default is 0, which means only the provided URL is considered).
- ext (list): A list of file extensions to filter the extracted files (e.g., ['.csv', '.xls']).

Returns:
- list: A list of extracted data file paths.

Note:
- If both url and path are provided, preference will be given to url.
- If no extension filter is provided (ext=None), all files will be considered.

Example:
```python
harmonizer = Harmonizer()

# Extract data from ur
url = "https://microdatos.dane.gov.co/index.php/catalog/548/get-microdata"
list_datainfo = harmonizer.extract(url=url, path=None, depth=0, ext=['.csv','.xls','.xlsx', ".txt", ".sav", ".zip"])
harmonizer = Harmonizer(list_datainfo)

# Extract data from path
url = "data/input"
list_datainfo = harmonizer.extract(url=None, path=url, depth=0, ext=['.csv'])
harmonizer = Harmonizer(list_datainfo)
```

## Transformation

Transforms the extracted data into a structured format.

Parameters:
-none

Returns:
- list: A list of transformed data (DataInfo object) file paths.

Example:
```python
harmonizer.transform()
```

## Load

Loads the extracted datasets.

Parameters:
-none

Returns:
- list: A list of tuples containing success status and message for each dataset.
        Each tuple has two elements: success (bool) and message (str).

Example:
```python
results = harmonizer.load()

# Print the results
for i, result in enumerate(results):
    print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")
```

## Querier

Queries the loaded datasets using SQL queries.

Create a querier intance
```python
from nyctibius.db.querier import Querier

querier = Querier(db_path='data/output/nyctibius.db')
```
### Start a SELECT query for a specific table and columns.
Select all columns from the table

parameters:
- table (str): The table name.
- columns (list, optional): A list of columns to be selected.

returns:
- Querier: The current Querier instance.

Example:
```python
df = querier.select(table="Estructura CHC_2017").execute()
```

Note: Select specific columns from the table
```python
df = querier.select(table="Estructura CHC_2017", columns=['DIRECTORIO', 'P1']).execute()
```

### Add a condition to the query.

parameters:
- condition (str): The condition to be added.
- operator (str, optional): The operator to be used to combine the condition with the previous one (default is 'AND').

returns:
- Querier: The current Querier instance.

Add a filter to the query
```python
df = querier.select(table="Estructura CHC_2017").filter("P1 = 1").execute()
```

Note: Add multiple filters to the query
```python
df = querier.select(table="Estructura CHC_2017").filter(condition="P1 > 5").filter(condition="P2 < 10", operator='AND').execute()
```

### Add an IN or NOT IN condition to the query.

parameters:
- column (str): The column name.
- values (list): A list of values to be checked.
- operator (str, optional): The operator to be used to combine the condition with the previous one (default is 'AND').
- not_in (bool, optional): If True, the condition will be NOT IN (default is False).

returns:
- Querier: The current Querier instance.

Add an IN condition to the query
```python
df = querier.select(table="Estructura CHC_2017").filter_in(column="P1", values=[1, 2, 3]).execute()
```

### Add a LIKE condition to the query that checks if a column starts with, ends with, or contains a pattern.

parameters:
- column (str): The column name.
- pattern (str): The pattern to be checked.
- condition_type (str): The type of LIKE condition. Must be 'startswith', 'endswith', or 'contains'.
- operator (str, optional): The operator to be used to combine the condition with the previous one (default is 'AND').

returns:
- Querier: The current Querier instance.

Add a LIKE condition to the query
```python
df = querier.select(table="Estructura CHC_2017").filter_like(column="P1", pattern="1%", condition_type='startswith').execute()
```

### Add a JOIN clause to the query.

parameters:
- table (str): The table name to be joined.
- join_type (str, optional): The type of join. Defaults to 'INNER'.
- on_condition (str, optional): The condition for the ON clause. Defaults to None.

returns:
- Querier: The current Querier instance.

Add a JOIN clause to the query
```python
df = querier.select(table="Estructura CHC_2017").join(table="Estructura_CHC_2017_Copy", on_condition="'Estructura CHC_2017.DIRECTORIO' = Estructura_CHC_2017_Copy.DIRECTORIO").execute()
```

### Add a LIMIT clause to the query

parameters:
- limit (int): The number of rows to be returned.

returns:
- Querier: The current Querier instance.

Add a LIMIT clause to the query
```python
df = querier.select(table="Estructura CHC_2017").limit(5).execute()
```

## Modifier

Initialize the Modifier instance
```python
from nyctibius.db.modifier import Modifier

modifier = Modifier(db_path='data/output/nyctibius.db')
```

### Get a list of all tables in the database

parameters:
- none

returns:
- list: A list of table names.

Example:
```python
tables = modifier.get_tables()
```

### Get a list of all columns in a table

parameters:
- table_name (str): The table name.

returns:
- list: A list of column names.

Example:
```python
columns = modifier.get_columns(table_name="Estructura CHC_2017")
```

### Get the datatypes of all columns in the table

parameters:
- table_name (str): The table name.

returns:
- dict: A dictionary with column names as keys and their datatypes as values.

Example:
```python
datatypes = modifier.get_column_datatypes(table_name="Estructura CHC_2017")
```

### Rename a table in the database

parameters:
- old_table_name (str): The old table name.
- new_table_name (str): The new table name.

returns:
- dict: A dictionary containing the status of the operation and a message.

Example:
```python
result = modifier.rename_table(old_table_name="Estructura CHC_2017", new_table_name="New_Estructura_CHC_2017")
```

### Rename a column in a table

parameters:
- table_name (str): The table name.
- old_column_name (str): The old column name.
- new_column_name (str): The new column name.

returns:
- dict: dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.rename_column(table_name="New_Estructura_CHC_2017", old_column_name="P1", new_column_name="New_P1")
```

### Rename the columns of a table.

parameters:
- table_name (str): The table name.
- new_column_names (list): A list of new column names.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.rename_table_columns(table_name="Estructura CHC_2017", new_column_names=["new_index", "new_DIRECTORIO", "new_TIP_FOR", "new_P1"])
```

### Remove duplicate rows in a table based on a specific column

parameters:
- table_name (str): The table name.
- column_name (str): The column name based on which duplicates are removed.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.remove_duplicates(table_name="New_Estructura_CHC_2017", column_name="New_P1")
```

### Set a column as the primary key of a table

parameters:
- table_name (str): The table name.
- pk_column_name (str): The name of the column to set as the primary key.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.set_primary_key(table_name="New_Estructura_CHC_2017", pk_column_name="New_P1")
```

### Delete a column from a table

parameters:
- table_name (str): The table name.
- column_name (str): The column name to be deleted.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.delete_column(table_name="Estructura CHC_2017", column_name="new_index")
```

### Duplicate a table in the database.

parameters:
- old_table_name (str): The old table name.
- new_table_name (str): The new table name.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.duplicate_table(old_table_name="Estructura CHC_2017", new_table_name="Estructura_CHC_2017_Copy")
```

### Set a column as a foreign key of a table

parameters: 
- table_name (str): The table name.
- fk_column_name (str): The name of the column to set as the foreign key.
- referenced_table_name (str): The name of the table that the foreign key references.
- referenced_column_name (str): The name of the column in the referenced table.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.set_foreign_key(table_name="Estructura CHC_2017", fk_column_name="new_DIRECTORIO", referenced_table_name="Other_Table", referenced_column_name="Estructura CHC_2017_copy")
```

Merge two tables into a new table

parameters:
- table1 (str): The name of the first table.
- table2 (str): The name of the second table.
- new_table (str): The name of the new table.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.merge_tables(table1="Estructura CHC_2017", table2="Other_Table", new_table="Merged_Table")
```

### Delete a table from the database

parameters:
- table_name (str): The table name to be deleted.

returns:
- dict: A dictionary containing 'status' and 'message'. 'status' is a boolean indicating whether the operation was successful or not. 'message' is a string describing the result of the operation.

Example:
```python
result = modifier.delete_table(table_name="New_Estructura_CHC_2017")
```