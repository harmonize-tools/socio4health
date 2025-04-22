
from dask.diagnostics import ProgressBar
from matplotlib.style.core import available

from socio4health import Extractor
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer, vertical_merge, drop_nan_columns, get_available_columns

col_extractor = Extractor(path="../../input/GEIH_2022/Original",down_ext=['.CSV','.csv','.zip'],sep=';', download_dir="data")
per_extractor = Extractor(path="../../input/ENAHO_2022/Original",down_ext=['.csv','.zip'], download_dir="data")
rd_extractor = Extractor(path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'], download_dir="data")
bra_extractor = Extractor(path="../../input/PNADC_2022/Original",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, download_dir="data")

def test(extractor):
    dfs = extractor.extract()
    dfs = vertical_merge(ddfs=dfs, similarity_threshold=0.9)
    drop_nan_columns(dfs, threshold=0.8)
    '''
    for df in dfs:
    with ProgressBar():
        print(df.head(npartitions=1))
    df.to_csv("data/output", index=False, single_file=True)
    '''
    available_columns = get_available_columns(dfs)
    print("Available columns:")
    print(available_columns)
    #selected_columns =
    #translate(dfs,dictionary)
    extractor.delete_download_folder()

if __name__ == "__main__":
    test(rd_extractor)