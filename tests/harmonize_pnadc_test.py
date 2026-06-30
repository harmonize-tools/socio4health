from pathlib import Path

import pandas as pd

from socio4health import Extractor, Harmonizer
from socio4health.utils import extractor_utils, harmonizer_utils
from socio4health.utils.harmonizer_utils import (
    group_and_onehot_encode,
    harmonize_columns_by_year,
    select_and_filter_columns,
)
from socio4health.utils.mapping_utils import load_int_key_mapping, load_json_mapping


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\OUTPUT"

_DATA_DIR = Path(__file__).resolve().parent / "pnadc_mapping"
COLUMN_MAPPING_BY_YEAR = load_json_mapping(_DATA_DIR, "column_mapping.json")["2012-2025"]
HARMONIZED_MAPPING = load_int_key_mapping(Path(__file__).resolve().parent, "harmonized_mapping.json")

fallback_col_cols = [
    "YEAR",
    "AREA",
    "H_TYPE",
    "H_WALLS",
    "H_FLOOR",
    "H_ROOF",
    "H_ELECTRICITY",
    "H_SANITARY",
    "H_GARBAGE",
    "H_WATER",
    "H_COOK",
    "H_PROPERTY",
    "P_LITERACY",
    "P_SEX",
    "P_ETHNIC",
    "P_AGE",
    "P_EDUCATION",
]

def main():
    PNADC_data = {
        2012: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2012",
        2013: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2013",
        2014: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2014",
        2015: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2015",
        2016: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2016",
        2017: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2017",
        2018: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2018",
        2019: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2019",
        2022: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2022",
        2023: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2023",
        2024: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2024",
        2025: r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\2025"
    }


    

    all_grouped_dfs = []
    
    raw_dic = pd.read_excel(r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\raw_pnadc_dict.xlsx")
    dic=harmonizer_utils.s4h_standardize_dict(raw_dic)
    colnames, colspecs = extractor_utils.s4h_parse_fwf_dict(dic)
    for year, path in PNADC_data.items():

        print(f"{year}: {path}")
        
        extractor = Extractor(
            input_path=path,
            is_fwf=True,
            down_ext=['.txt'],
            output_path=OUTPUT_PATH,
            colnames=colnames,
            colspecs=colspecs
        )
        dfs_extracted = extractor.s4h_extract()
        for df in dfs_extracted:
            df['YEAR'] = year
        for df in dfs_extracted:
            print(f"Extracted DataFrame columns: {list(df.columns)}")

        har = Harmonizer()
        dfs = har.s4h_vertical_merge(dfs_extracted, overlap_threshold=0.9, method="union")

        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)

        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]

        for extra_col in fallback_col_cols:
            if any(extra_col in df.columns for df in dfs) and extra_col not in available_harmonized:
                available_harmonized.append(extra_col)

        for required_col in ("YEAR", "ADMIN_DIVISION_2", "EXP_FACTOR", "UPA"):
            if any(required_col in df.columns for df in dfs) and required_col not in available_harmonized:
                available_harmonized.append(required_col)

        dfs = select_and_filter_columns(dfs, available_harmonized, num_cols_threshold=1)

        metadata_cols = {'YEAR', 'ADMIN_DIVISION_2', 'EXP_FACTOR', 'UPA'}
        dfs = [df for df in dfs if any(col not in metadata_cols for col in df.columns)]

        print(f"Number of valid DataFrames for grouping: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")

        grouped_dfs = group_and_onehot_encode(
            dfs,
            group_col='ADMIN_DIVISION_2',
            weight_col='EXP_FACTOR',
            id_col='UPA',
            value_labels_by_column=HARMONIZED_MAPPING,
        )
        all_grouped_dfs.extend(grouped_dfs)

    if all_grouped_dfs:
        final_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/PNADC_harmonized.csv", index=False)
    else:
        print("No data to save.")
    

if __name__ == "__main__":
    main()

