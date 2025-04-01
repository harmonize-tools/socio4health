from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer

dfs_col = Harmonizer().extract(path="../../input/GEIH_2022/Test",down_ext=['.csv','.zip'],sep=';')
print(dfs_col)
dfs_per = Harmonizer().extract(path="../../input/ENAHO_2022/Test",down_ext=['.csv','.zip'])
print(dfs_per)
dfs_rd = Harmonizer().extract(path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'])
print(dfs_rd)
dfs_bra = Harmonizer().extract(path="../../input/PNADC_2022/Test",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, delete_data_dir=False)
print(dfs_bra)

print(dfs_col[0].columns.to_list())