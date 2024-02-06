from nyctibius.sql.querier import Querier

querier = Querier()

tables = querier.get_tables()
print(tables)

columns = querier.get_columns('CNPV2018_3FALL_A2_05')
print(columns)

print(querier.rename_table("CNPV2018_3FALL_A2_05", "Deceased"))

print(querier.rename_column("Deceased", "TIPO_REG", "type"))

print(querier.rename_table_columns("Deceased", ['id', 'type', 'Department', 'Municipality', 'class', 'sourvey_id',
                                   'living', 'housing_id', 'num_fall', 'sex_fall', 'age_fall', 'death_cert']))

print(querier.set_primary_key("Deceased", "id"))

print(querier.set_foreign_key("Deceased", "sourvey_id", "CNPV2018_1VIV_A2_05", "COD_ENCUESTAS"))

query = """
CREATE TABLE NewTable AS
SELECT *
FROM Deceased
JOIN CNPV2018_1VIV_A2_05
ON Deceased.sourvey_id = CNPV2018_1VIV_A2_05.COD_ENCUESTAS
"""

# Assuming 'connection' is your database connection
# Call the function with the connection and the query
results = querier.execute_query(query)
print(results)
