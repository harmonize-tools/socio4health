import datetime
import pandas as pd
from socio4health import Extractor
from socio4health.harmonizer import Harmonizer
from socio4health.utils import harmonizer_utils
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum

col_extractor_test = Extractor(input_path="../../input/GEIH_2022/Test",down_ext=['.CSV'],sep=';', output_path="data")

col_extractor = Extractor(input_path="../../input/GEIH_2022/Original",down_ext=['.CSV','.csv','.zip'],sep=';', output_path="data")
per_extractor = Extractor(input_path="../../input/ENAHO_2022/Original",down_ext=['.csv','.zip'], output_path="data")
rd_extractor = Extractor(input_path="../../input/ENHOGAR_2022/Original",down_ext=['.csv','.zip'], output_path="data")
bra_extractor = Extractor(input_path="../../input/PNADC_2022/Original",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, output_path="data")

col_online_extractor = Extractor(input_path="https://microdatos.dane.gov.co/index.php/catalog/771/get-microdata",down_ext=['.CSV','.csv','.zip'],sep=';', output_path="data", depth=0)
per_online_extractor = Extractor(input_path="https://www.inei.gob.pe/media/DATOS_ABIERTOS/ENAHO/DATA/2022.zip",down_ext=['.csv','.zip'], output_path="data", depth=0)
rd_online_extractor = Extractor(input_path="https://www.one.gob.do/datos-y-estadisticas/",down_ext=['.csv','.zip'], output_path="data", depth=0, key_words=["ENH22"])
bra_online_extractor = Extractor(input_path="https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2024/",down_ext=['.txt','.zip'],is_fwf=True,colnames=BraColnamesEnum.PNADC.value, colspecs=BraColspecsEnum.PNADC.value, output_path="data", depth=0)

raw_dict = pd.read_excel('../Socio4HealthData/Dictionaries/Brazil/Diccionario Crudo.xlsx')

def test(extractor):
    first_date = datetime.datetime.now()

    dfs = extractor.extract()
    print("Extractor")
    print(datetime.datetime.now() - first_date )
    second_date = datetime.datetime.now()

    har = Harmonizer()
    har.similarity_threshold = 0.9
    har.nan_threshold = 1
    dfs = har.vertical_merge(dfs)
    dfs = har.drop_nan_columns(dfs)
    # print(dfs.columns)
    print("Harmonizer")
    print(datetime.datetime.now() - second_date)
    third_date = datetime.datetime.now()


    dic = harmonizer_utils.standardize_dict(raw_dict)
    dic = harmonizer_utils.translate_column(dic, "question", language="en")
    dic = harmonizer_utils.translate_column(dic, "description", language="en")
    dic = harmonizer_utils.translate_column(dic, "possible_answers", language="en")
    dic = harmonizer_utils.classify_rows(dic, "question_en", "description_en", "possible_answers_en", new_column_name="category", MODEL_PATH="../Socio4HealthData/input/bert_finetuned_classifier")
    print("Dictionary")
    print(datetime.datetime.now() - third_date)
    
    fourth_date = datetime.datetime.now()

    har.dict_df = dic
    har.categories = ["Business","Education"]
    har.key_col = 'rm_ride'
    har.key_val = ['22','25']
    filtered_ddfs = har.data_selector(dfs)
    for i, df in enumerate(filtered_ddfs):
        df.to_csv(f"data/output_{i}.csv", index=False)
    #extractor.delete_download_folder()
    print("Data selector")
    print(datetime.datetime.now() - fourth_date)
    
    print("Total")
    fifth_date = datetime.datetime.now()
    interval = fifth_date - first_date
    print(interval)
if __name__ == "__main__":
    test(bra_online_extractor)