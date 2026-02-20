# Quick test for s4h_join_data composite key support
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from socio4health.harmonizer import Harmonizer
import pandas as pd
import dask.dataframe as dd

# Minimal person-level (df1) and household-level (df2) samples
cols_person = ['AÑO','MES','CONGLOME','VIVIENDA','HOGAR','CODPERSO','UBIGEO','DOMINIO','ESTRATO','P201P','TICUEST01','NCONGLOME','SUB_CONGLOME']
cols_house = ['AÑO','MES','CONGLOME','VIVIENDA','HOGAR','UBIGEO','DOMINIO','ESTRATO','PERIODO','TICUEST01','NCONGLOME','SUB_CONGLOME','P22']

person = pd.DataFrame([
    [2022,'01','005030','008','11','01','010201','7','4',20220050300081101,'006618','00','00'],
    [2022,'01','005030','008','11','02','010201','7','4',20220050300081102,'006618','00','00']
], columns=cols_person)

house = pd.DataFrame([
    [2022,'01','005030','008','11','010201','7','4',1,'006618','00','00', 123],
], columns=cols_house)

# Convert to Dask DataFrames
ddf_person = dd.from_pandas(person, npartitions=1)
ddf_house = dd.from_pandas(house, npartitions=1)

h = Harmonizer()
# Use composite join key matching the sample columns
h.join_key = ['AÑO','MES','CONGLOME','VIVIENDA','HOGAR','UBIGEO','DOMINIO','ESTRATO','TICUEST01','NCONGLOME','SUB_CONGLOME']

merged = h.s4h_join_data([ddf_person, ddf_house])
print('\nCOLUMNS:')
print(list(merged.columns))
print('\nROWS:')
print(merged.to_dict(orient='records'))
