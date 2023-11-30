import os

import pandas as pd
import dask.dataframe as dd
from dask.utils import tmpfile
from sqlalchemy import create_engine, text

df = pd.DataFrame([{'i': i, 's': str(i) * 2} for i in range(4)])
ddf = dd.from_pandas(df, npartitions=2)

absolute_path = os.path.abspath('../Nyctibius/data/output/nyctibius.db')
absolute_path = absolute_path.replace('\\', '/')
print("Absolute Path:", absolute_path)
db = 'sqlite:///%s' % 'D:/Documents/GitHub/Nyctibius/data/output/nyctibius.db'
ddf.to_sql('test', db, )
engine = create_engine(db, echo=False)
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM test")).fetchall()
print(result)
