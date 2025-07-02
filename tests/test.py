import pandas as pd

from socio4health import Extractor
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer

col_extractor_test = Extractor(input_path="../../input/GEIH_2022/Test",down_ext=['.CSV'],sep=';', output_path="data")

col_extractor = Extractor(input_path="../../input/GEIH_2022/Original",down_ext=['.CSV','.csv','.zip'],sep=';', output_path="data")
per_extractor = Extractor(input_path="../../input/ENAHO_2022/Original",down_ext=['.csv','.zip'], output_path="data")
rd_extractor = Extractor(input_path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'], output_path="data")
bra_extractor = Extractor(input_path="../../input/PNADC_2022/Original",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, output_path="data")

col_online_extractor = Extractor(input_path="https://microdatos.dane.gov.co/index.php/catalog/771/get-microdata",down_ext=['.CSV','.csv','.zip'],sep=';', output_path="data", depth=0)
per_online_extractor = Extractor(input_path="https://www.inei.gob.pe/media/DATOS_ABIERTOS/ENAHO/DATA/2022.zip",down_ext=['.csv','.zip'], output_path="data", depth=0)
rd_online_extractor = Extractor(input_path="https://www.one.gob.do/datos-y-estadisticas/",down_ext=['.csv','.zip'], output_path="data", depth=0, key_words=["ENH22"])
bra_online_extractor = Extractor(input_path="https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2024/",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, output_path="data", depth=0)

col_dict = pd.read_excel('../../input/GEIH_2022/DiccionarioFinal.xlsx')

def test(extractor):
    dfs = extractor.extract()
    har = Harmonizer()
    har.similarity_threshold = 0.9
    har.nan_threshold = 1
    dfs = har.vertical_merge(dfs)
    dfs = har.drop_nan_columns(dfs)

    har.dict_df = col_dict
    har.categories = ["Business","Education"]
    har.key_col = 'DPTO'
    har.key_val = ['05','25']
    filtered_ddfs = har.data_selector(dfs)
    for i, df in enumerate(filtered_ddfs):
        df.to_csv(f"data/output_{i}.csv", index=False)
    #extractor.delete_download_folder()

if __name__ == "__main__":
    test(col_extractor_test)