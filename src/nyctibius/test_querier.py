# Example Usage of Querier Class
from nyctibius.sql.querier import Querier

# Create an instance of Querier
querier = Querier()

# Example 1: Execute a SQL Query
query = "CREATE TABLE IF NOT EXISTS example_table (id INTEGER PRIMARY KEY, name TEXT);"
success, error = querier.execute_query(query)
if success:
    print("Query executed successfully")
else:
    print(f"Error executing query: {error}")

# Example 2: Get a List of Tables
result = querier.get_tables()

if isinstance(result, tuple) and result[0] is False:
    # Handle the error and print the error message
    print(f"Error getting tables: {result[1]}")
else:
    # Unpack the list of table names
    tables = result
    print("Tables in the database:", tables)

# Example 3: Get Columns for a Table
table_name = "example_table"
columns, error = querier.get_columns(table_name)
if columns:
    print(f"Columns in table '{table_name}':", columns)
else:
    print(f"Error getting columns: {error}")

# Example 4: Rename a Table
initial_name = "example_table"
final_name = "renamed_table"
success, error = querier.rename_table(initial_name, final_name)
if success:
    print(f"Table '{initial_name}' renamed to '{final_name}' successfully")
else:
    print(f"Error renaming table: {error}")

# Example 5: Rename a Column
table_name = "renamed_table"  # Use the renamed table from Example 4
initial_name = "name"
final_name = "new_name"
success, error = querier.rename_column(table_name, initial_name, final_name)
if success:
    print(f"Column '{initial_name}' in table '{table_name}' renamed to '{final_name}' successfully")
else:
    print(f"Error renaming column: {error}")

# Example 7: Set Column as Not Nullable
table_name = "renamed_table"
column_name = "new_name"
is_nullable = False
success, error = querier.set_column_not_nullable(table_name, column_name, is_nullable)
if success:
    print(f"Column '{column_name}' set as {'nullable' if is_nullable else 'not nullable'} in table '{table_name}'")
else:
    print(f"Error setting column as {'nullable' if is_nullable else 'not nullable'}: {error}")

# Example 6: Set Primary Key
table_name = "renamed_table"
column_name = "id"
success, error = querier.set_primary_key(table_name, column_name)
if success:
    print(f"Primary key set successfully for column '{column_name}' in table '{table_name}'")
else:
    print(f"Error setting primary key: {error}")

# Example 8: Set Foreign Key Constraint
column_name = "foreign_key_column"
main_table = "main_table"
secondary_table = "referenced_table"
success, error = querier.set_foreign_key(column_name, main_table, secondary_table)
if success:
    print(
        f"Foreign key constraint set for column '{column_name}' in table '{main_table}' referencing table '{secondary_table}'")
else:
    print(f"Error setting foreign key: {error}")