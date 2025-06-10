

from socio4health import Extractor
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import vertical_merge, drop_nan_columns, get_available_columns

col_extractor_test = Extractor(input_path="../../input/GEIH_2022/Test",down_ext=['.CSV'],sep=';', output_path="data")

col_extractor = Extractor(input_path="../../input/GEIH_2022/Original",down_ext=['.CSV','.csv','.zip'],sep=';', output_path="data")
per_extractor = Extractor(input_path="../../input/ENAHO_2022/Original",down_ext=['.csv','.zip'], output_path="data")
rd_extractor = Extractor(input_path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'], output_path="data")
bra_extractor = Extractor(input_path="../../input/PNADC_2022/Original",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, output_path="data")

col_online_extractor = Extractor(input_path="https://microdatos.dane.gov.co/index.php/catalog/771/get-microdata",down_ext=['.CSV','.csv','.zip'],sep=';', output_path="data", depth=0)
per_online_extractor = Extractor(input_path="https://www.inei.gob.pe/media/DATOS_ABIERTOS/ENAHO/DATA/2022.zip",down_ext=['.csv','.zip'], output_path="data", depth=0)
rd_online_extractor = Extractor(input_path="https://www.one.gob.do/datos-y-estadisticas/",down_ext=['.csv','.zip'], output_path="data", depth=0, key_words=["ENH22"])
bra_online_extractor = Extractor(input_path="https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2024/",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, output_path="data", depth=0)

def test(extractor):
    dfs = extractor.extract()
    dfs = vertical_merge(ddfs=dfs, similarity_threshold=0.9)
    dfs = drop_nan_columns(dfs, threshold=0.8)
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
    test(col_extractor_test)