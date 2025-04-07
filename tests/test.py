from socio4health import Transformer
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer

dfs_col = Harmonizer().extract(path="../../input/GEIH_2022/Processed",down_ext=['.csv','.zip'],sep=';')
#dfs_col = Harmonizer().extract(path="../../input/GEIH_2022",down_ext=['.csv','.zip'],sep=';')

transformer = Transformer(dataframes=dfs_col)
dfs_col = transformer.vertical_merge(fill_value=None, sort_columns=False, similarity_threshold=0.9, nan_threshold=0.9)
for df in dfs_col:
    print(df)

#dfs_per = Harmonizer().extract(path="../../input/ENAHO_2022/Test",down_ext=['.csv','.zip'])
#print(dfs_per)
#dfs_rd = Harmonizer().extract(path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'])
#print(dfs_rd)
#dfs_bra = Harmonizer().extract(path="../../input/PNADC_2022/Test",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, delete_data_dir=False)
#print(dfs_bra)

