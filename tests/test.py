import datetime
from zoneinfo import available_timezones

import pandas as pd

from socio4health.utils.harmonizer_utils import standardize_dict
from socio4health import Extractor
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer
from socio4health.utils import harmonizer_utils
from socio4health.utils.extractor_utils import parse_fwf_dict

col_extractor_test = Extractor(input_path="../../input/GEIH_2022/sept",down_ext=['.csv','.zip'],sep=';', output_path="data")

col_extractor = Extractor(input_path="../../input/GEIH_2022/Original",down_ext=['.csv','.zip'],sep=';', output_path="data")
per_extractor = Extractor(input_path="../../input/ENAHO_2022/Original",down_ext=['.csv','.zip'], output_path="data")
rd_extractor = Extractor(input_path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'], output_path="data")
#bra_extractor = Extractor(input_path="../../input/PNADC_2022/Original",down_ext=['.txt','.zip'],is_fwf=True,output_path="data")

col_online_extractor = Extractor(input_path="https://microdatos.dane.gov.co/index.php/catalog/771/get-microdata",down_ext=['.csv','.zip'],sep=';', output_path="data", depth=0)
per_online_extractor = Extractor(input_path="https://www.inei.gob.pe/media/DATOS_ABIERTOS/ENAHO/DATA/2022.zip",down_ext=['.csv','.zip'], output_path="data", depth=0)
rd_online_extractor = Extractor(input_path="https://www.one.gob.do/datos-y-estadisticas/",down_ext=['.csv','.zip'], output_path="data", depth=0, key_words=["ENH22"])
#bra_online_extractor = Extractor(input_path="https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2024/",down_ext=['.txt','.zip'],is_fwf=True, output_path="data", depth=0)

col_dict = pd.read_excel('../../input/GEIH_2022/DiccionarioFinal.xlsx')
raw_dict = pd.read_excel('../../input/PNADC_2022/DiccionarioCrudo.xlsx')

def test():
    har = Harmonizer()

    '''
    dic = harmonizer_utils.standardize_dict(raw_dict)
    dic = harmonizer_utils.translate_column(dic, "question", language="en")
    dic = harmonizer_utils.translate_column(dic, "description", language="en")
    dic = harmonizer_utils.translate_column(dic, "possible_answers", language="en")
    dic = harmonizer_utils.classify_rows(dic, "question_en", "description_en", "possible_answers_en",
                                         new_column_name="category",
                                         MODEL_PATH="../../input/bert_finetuned_classifier")
    '''
    # = pd.read_excel('../../input/PNADC_2022/PNADC_2022_Dictionary.xlsx')
    #dic.to_excel('data/PNADC_2022_Dictionary.xlsx', index=False)
    #colnames, colspecs = parse_fwf_dict(dic)

    #extractor = Extractor(input_path="../../input/PNADC_2022/Test", down_ext=['.txt', '.zip'], is_fwf=True,
    #                      colnames = colnames, colspecs = colspecs,output_path="data")

    extractor = col_online_extractor
    har.dict_df = col_dict

    dfs = extractor.extract()
    har.similarity_threshold = 0.9

    har.join_key = 'DIRECTORIO'
    har.aux_key = 'ORDEN'
    har.extra_cols = ['ORDEN']

    print('Vertical merge_____________________________________')
    dfs = har.vertical_merge(dfs)

    # har.nan_threshold = 1
    #dfs = har.drop_nan_columns(dfs)

    for df in dfs:
        print(f"DataFrame shape: {df.shape}")
        print(df.head())

    har.categories = ["Business"]
    har.key_col = 'DPTO'
    har.key_val = ['11']
    print('Data harmonization_________________________________')
    filtered_dask_dfs = har.data_selector(dfs)

    print(filtered_dask_dfs[0].head())


'''
    print('Horizontal merge___________________________________')
    joined_df = har.join_data(filtered_ddfs)
    available_cols = joined_df.columns.tolist()
    print(f"Available columns: {available_cols}")
    print(f"Shape of the joined DataFrame: {joined_df.shape}")
    print(joined_df.head())
    joined_df.to_csv('data/GEIH_2022_harmonized.csv', index=False)
'''

if __name__ == "__main__":
    test()