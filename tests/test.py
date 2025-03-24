from src.socio4health.harmonizer import Harmonizer

dfs_col = Harmonizer().extract(path="../../input/GEIH_2022/Test",down_ext=['.csv','.zip'])
print(dfs_col)
dfs_per = Harmonizer().extract(path="../../input/ENAHO_2022/Test",down_ext=['.csv','.zip'])
print(dfs_per)
dfs_rd = Harmonizer().extract(path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'])
print(dfs_rd)
#bra_harmonizer = Harmonizer()
#bra_harmonizer.country = 'BRA'
#bra_harmonizer.year = 2022
#bra_harmonizer.data_source_type = 'PNADC'
#dfs_bra = bra_harmonizer.extract(path="../../input/PNADC_2022/Test",down_ext=['.txt','.zip'])