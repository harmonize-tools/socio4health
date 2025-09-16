import os
import datetime
from zoneinfo import available_timezones

import pandas as pd

from socio4health import Extractor
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer
from socio4health.utils import harmonizer_utils
from socio4health.utils.extractor_utils import s4h_parse_fwf_dict
from socio4health.utils.harmonizer_utils import s4h_standardize_dict


print(f"Current working directory: {os.getcwd()}")
print(f"Script location: {os.path.dirname(os.path.abspath(__file__))}")

file_path = '../input/GEIH_2022/DiccionarioFinal.xlsx'
print(f"Looking for file at: {os.path.abspath(file_path)}")
print(f"File exists: {os.path.exists(file_path)}")

# Initialize extractors for different datasets
col_extractor_test = Extractor(
    input_path="../input/GEIH_2022/sept",
    down_ext=['.csv', '.zip'],
    sep=';',
    output_path="data"
)

col_extractor = Extractor(
    input_path="../input/GEIH_2022/Original",
    down_ext=['.csv', '.zip'],
    sep=';',
    output_path="data"
)

per_extractor = Extractor(
    input_path="../input/ENAHO_2022/Original",
    down_ext=['.csv', '.zip'],
    output_path="data"
)

rd_extractor = Extractor(
    input_path="../input/ENHOGAR_2022/Original",
    down_ext=['.csv', '.zip'],
    output_path="data"
)

# Online extractors
col_online_extractor = Extractor(
    input_path="https://microdatos.dane.gov.co/index.php/catalog/771/get-microdata",
    down_ext=['.csv', '.zip'],
    sep=';',
    output_path="data",
    depth=0
)

per_online_extractor = Extractor(
    input_path="https://www.inei.gob.pe/media/DATOS_ABIERTOS/ENAHO/DATA/2022.zip",
    down_ext=['.csv', '.zip'],
    output_path="data",
    depth=0
)

rd_online_extractor = Extractor(
    input_path="https://www.one.gob.do/datos-y-estadisticas/",
    down_ext=['.csv', '.zip'],
    output_path="data",
    depth=0,
    key_words=["ENH22"]
)

# Load dictionaries
col_dict = pd.read_excel('../input/GEIH_2022/DiccionarioFinal.xlsx')
raw_dict = pd.read_excel('../input/PNADC_2022/DiccionarioCrudo.xlsx')


def test():
    """Test function for data harmonization pipeline."""
    har = Harmonizer()

    """
    dic = harmonizer_utils.standardize_dict(raw_dict)
    dic = harmonizer_utils.translate_column(dic, "question", language="en")
    dic = harmonizer_utils.translate_column(dic, "description", language="en")
    dic = harmonizer_utils.translate_column(dic, "possible_answers", language="en")
    dic = harmonizer_utils.classify_rows(
        dic, 
        "question_en", 
        "description_en", 
        "possible_answers_en",
        new_column_name="category",
        MODEL_PATH="../../input/bert_finetuned_classifier"
    )
    """

    extractor = col_online_extractor
    har.dict_df = col_dict
    har.similarity_threshold = 0.9

    har.join_key = 'DIRECTORIO'
    har.aux_key = 'ORDEN'
    har.extra_cols = ['ORDEN']

    print('Extracting data...')
    dfs = extractor.s4h_extract()

    print('Vertical merge_____________________________________')
    dfs = har.s4h_vertical_merge(dfs)

    for i, df in enumerate(dfs):
        print(f"DataFrame {i + 1} shape: {df.shape}")
        print(df.head())
        print("-" * 50)

    har.categories = ["Business"]
    har.key_col = 'DPTO'
    har.key_val = ['11']

    print('Data harmonization_________________________________')
    filtered_dask_dfs = har.s4h_data_selector(dfs)

    print(filtered_dask_dfs[0].head())

    """
    print('Horizontal merge___________________________________')
    joined_df = har.join_data(filtered_ddfs)
    available_cols = joined_df.columns.tolist()

    print(f"Available columns: {available_cols}")
    print(f"Shape of the joined DataFrame: {joined_df.shape}")
    print(joined_df.head())

    # Save results
    joined_df.to_csv('data/GEIH_2022_harmonized.csv', index=False)
    """

    extractor.s4h_delete_download_folder()

if __name__ == "__main__":
    test()