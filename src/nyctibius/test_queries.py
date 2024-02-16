from nyctibius.db.modifier import Modifier
from nyctibius.db.querier import Querier

modifier = Modifier(db_path='../../data/output/nyctibius.db')
querier = Querier(db_path='../../data/output/nyctibius.db')

print(modifier.get_tables())
print(modifier.get_columns('Estructura CHC_2017'))

df = querier.select(table="Estructura CHC_2017", columns=['DIRECTORIO', 'P1'], limit=5)
print(df)

#result = querier.select('*').from_('orders').where('price > 100').and_('quantity > 10').or_('discount > 0').limit(50).execute()