from nyctibius.db.modifier import Modifier
from nyctibius.db.querier import Querier

querier = Querier(db_path='../../data/output/nyctibius.db')

# Initialize the Querier
querier = Querier(db_path='../../data/output/nyctibius.db')

# Select all columns from the table
df = querier.select(table="Estructura CHC_2017").execute()
print(df)

# Select specific columns from the table
df = querier.select(table="Estructura CHC_2017", columns=['DIRECTORIO', 'P1']).execute()
print(df)

# Add a filter to the query
df = querier.select(table="Estructura CHC_2017").filter(condition="P1 > 5").execute()
print(df)

# Add multiple filters to the query
df = querier.select(table="Estructura CHC_2017").filter(condition="P1 > 5").filter(condition="P2 < 10",
                                                                                   operator='AND').execute()
print(df)

# Add an IN condition to the query
df = querier.select(table="Estructura CHC_2017").filter_in(column="P1", values=[1, 2, 3]).execute()
print(df)

# Add a LIKE condition to the query
df = querier.select(table="Estructura CHC_2017").filter_like(column="P1", pattern="1%",
                                                             condition_type='startswith').execute()
print(df)

# Add a LIMIT clause to the query
df = querier.select(table="Estructura CHC_2017").limit(5).execute()
print(df)

# ________________________________________________________________________________

# Initialize the Modifier
modifier = Modifier(db_path='../../data/output/nyctibius.db')

# Get the datatypes of all columns in the table
datatypes = modifier.get_column_datatypes(table_name="Estructura CHC_2017")
print(datatypes)

# Get a list of all tables in the database
tables = modifier.get_tables()
print(tables)

# Get a list of all columns in a table
columns = modifier.get_columns(table_name="Estructura CHC_2017")
print(columns)

# Rename a table in the database
result = modifier.rename_table(old_table_name="Estructura CHC_2017", new_table_name="New_Estructura_CHC_2017")
print(result)

# Rename a column in a table
result = modifier.rename_column(table_name="New_Estructura_CHC_2017", old_column_name="P1", new_column_name="New_P1")
print(result)

# Remove duplicate rows in a table based on a specific column
result = modifier.remove_duplicates(table_name="New_Estructura_CHC_2017", column_name="New_P1")
print(result)

# Set a column as the primary key of a table
result = modifier.set_primary_key(table_name="New_Estructura_CHC_2017", pk_column_name="New_P1")
print(result)

# Rename the columns of a table
result = modifier.rename_table_columns(table_name="Estructura CHC_2017", new_column_names=["new_index", "new_DIRECTORIO", "new_TIP_FOR", "new_P1"])
print(result)

# Delete a column from a table
result = modifier.delete_column(table_name="Estructura CHC_2017", column_name="new_index")
print(result)

# Duplicate a table
result = modifier.duplicate_table(old_table_name="Estructura CHC_2017", new_table_name="Estructura_CHC_2017_Copy")
print(result)

# Set a column as a foreign key of a table
result = modifier.set_foreign_key(table_name="Estructura CHC_2017", fk_column_name="new_DIRECTORIO", referenced_table_name="Other_Table", referenced_column_name="Other_Column")
print(result)

# Merge two tables into a new table
result = modifier.merge_tables(table1="Estructura CHC_2017", table2="Other_Table", new_table="Merged_Table")
print(result)

# Delete a table from the database
result = modifier.delete_table(table_name="New_Estructura_CHC_2017")
print(result)
