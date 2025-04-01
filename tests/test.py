import os

from socio4health import Transformer
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer

#dfs_col = Harmonizer().extract(path="../../input/GEIH_2022/Test",down_ext=['.csv','.zip'],sep=';')
dfs_col = Harmonizer().extract(path="../../input/GEIH_2022",down_ext=['.csv','.zip'],sep=';')
print(dfs_col)
#dfs_per = Harmonizer().extract(path="../../input/ENAHO_2022/Test",down_ext=['.csv','.zip'])
#dfs_rd = Harmonizer().extract(path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'])
#dfs_bra = Harmonizer().extract(path="../../input/PNADC_2022/Test",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, delete_data_dir=False)

#dict_path = "../src/dict/dictionary.csv"
#dictionary =
transformer = Transformer(raw_dataframes=dfs_col, key_column="DIRECTORIO", merge=True)
dfs_col = transformer.merge_dfs()
print(dfs_col)