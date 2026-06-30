
from pathlib import Path

import pandas as pd

from socio4health import Harmonizer
from socio4health.utils.harmonizer_utils import (
    apply_value_mappings,
    extract_and_prepare_data,
    group_and_onehot_encode,
    harmonize_columns_by_year,
    merge_factor,
    select_and_filter_columns,
)
from socio4health.utils.mapping_utils import load_int_key_mapping, load_json_mapping


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\OUTPUT"

_DATA_DIR = Path(__file__).resolve().parent / "per_mapping"
COLUMN_MAPPING_BY_YEAR = load_json_mapping(_DATA_DIR, "column_mapping.json")["2004-2024"]
HARMONIZED_MAPPING = load_int_key_mapping(Path(__file__).resolve().parent, "harmonized_mapping.json")
ENAHO_VALUE_MAPPING = load_int_key_mapping(_DATA_DIR, "enaho_mapping.json")["mapping"]

fallback_col_cols = [
        "YEAR",
        "VIVIENDA",
        "UBIGEO",
        "FACTOR07",
        "ESTRATO",
        "P101",
        "P102",
        "P103",
        "P103A",
        "P1121",
        "P1123",
        "P1124",
        "P1125",
        "P1126",
        "P1127",
        "P111A",
        "P110",
        "P113A",
        "P105A",
        "P5000",
        "P5010",
        "P302",
        "P207",
        "P558C",
        "P208A",
        "P301A"
    ]



def main():
    GEIH_data = {
        2004: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2004",
        2005: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2005",
        2006: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2006",
        2007: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2007",
        2008: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2008",
        2009: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2009",
        2010: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2010",
        2011: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2011",
        2012: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2012",
        2013: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2013",
        2014: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2014",
        2015: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2015",
        2016: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2016",
        2017: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2017",
        2018: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2018",
        2019: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2019",
        2020: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2020",
        2021: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2021",
        2022: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2022",
        2023: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2023",
        2024: r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\2024"
    }
    all_grouped_dfs = []
    for year, path in GEIH_data.items():
        ddfs = extract_and_prepare_data(year, path, ext='.sav', sep=';', output_path=OUTPUT_PATH)
        har = Harmonizer()
        dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.9, method="union")

        dfs = merge_factor(dfs, factor_col='FACTOR07', id_col='VIVIENDA')

        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)

        print(f"\nAplicando mapeos de valores para año {year}...")
        dfs = apply_value_mappings(dfs, year, ENAHO_VALUE_MAPPING, column_aliases=COLUMN_MAPPING_BY_YEAR)

        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]

        if not available_harmonized:
            available_harmonized = [
                col for col in fallback_col_cols
                if any(col in df.columns for df in dfs)
            ]

        for required_col in ("YEAR", "ADMIN_DIVISION", "EXP_FACTOR"):
            if any(required_col in df.columns for df in dfs) and required_col not in available_harmonized:
                available_harmonized.append(required_col)

        dfs = select_and_filter_columns(dfs, available_harmonized, num_cols_threshold=1)

        metadata_cols = {'YEAR', 'ADMIN_DIVISION', 'EXP_FACTOR', 'ID'}
        dfs = [df for df in dfs if any(col not in metadata_cols for col in df.columns)]

        print(f"Number of valid DataFrames for grouping: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")

        grouped_dfs = group_and_onehot_encode(
            dfs,
            group_col='ADMIN_DIVISION',
            weight_col='EXP_FACTOR',
            id_col='ID',
            value_labels_by_column=HARMONIZED_MAPPING,
        )

        all_grouped_dfs.extend(grouped_dfs)

    if all_grouped_dfs:
        print("\nCombining and aligning Household and Individual data horizontally...")

        combined_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df = combined_df.groupby(['YEAR', 'ADMIN_DIVISION'], as_index=False, dropna=False).first()
        final_df = final_df.sort_values(by=['YEAR', 'ADMIN_DIVISION']).reset_index(drop=True)

        ordered_cols = ['YEAR', 'ADMIN_DIVISION'] + [col for col in final_df.columns if col not in ['YEAR', 'ADMIN_DIVISION']]
        final_df = final_df[ordered_cols]

        output_file = f"{OUTPUT_PATH}\\ENAHO_harmonized.csv"
        final_df.to_csv(output_file, index=False)
        print(f"Successfully saved final harmonized data to {output_file}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
        

