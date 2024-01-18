from nyctibius.sql.querier import Querier

querier = Querier()

tables = querier.get_tables()

if tables['status'] == 'success':
    # If the operation was successful, print the list of tables
    print("Tables in the database:")
    for table in tables['result']:
        print(table[0])
else:
    # If the operation failed, print the error message
    print("Error getting tables:", tables['error'])

columns = querier.get_columns('CNPV2018_3FALL_A2_05')

if columns['status'] == 'success':
    # If the operation was successful, print the list of columns
    print("Columns in the table:")
    for column in columns['result']:
        print(column[1])
else:
    # If the operation failed, print the error message
    print("Error getting columns:", columns['error'])

#print(querier.rename_table("CNPV2018_3FALL_A2_05", "Deceased"))

#print(querier.rename_column("Deceased", "TIPO_REG", "type"))

#print(querier.rename_table_columns("Deceased", ['id', 'type', 'Department', 'Municipality', 'class', 'sourvey_id',
#                                   'living', 'housing_id', 'num_fall', 'sex_fall', 'age_fall', 'death_cert']))

print(querier.set_primary_key("Deceased", "id"))

print(querier.set_foreign_key("Deceased", "sourvey_id", "CNPV2018_1VIV_A2_05", "COD_ENCUESTAS"))
