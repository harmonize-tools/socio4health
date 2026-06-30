from pathlib import Path

import pandas as pd

from socio4health import Extractor, Harmonizer
from socio4health.utils import extractor_utils, harmonizer_utils
from socio4health.utils.harmonizer_utils import (
    apply_value_mappings,
    group_and_onehot_encode,
    harmonize_columns_by_year,
    merge_factor,
    select_and_filter_columns,
)
from socio4health.utils.mapping_utils import load_json_mapping, load_int_key_mapping


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\OUTPUT"

_DATA_DIR = Path(__file__).resolve().parent / "pnad_mapping"
COLUMN_MAPPING_BY_YEAR = load_json_mapping(_DATA_DIR, "column_mapping.json")["2001-2011"]
HARMONIZED_MAPPING = load_int_key_mapping(Path(__file__).resolve().parent, "harmonized_mapping.json")
PNAD_VALUE_MAPPING = load_int_key_mapping(_DATA_DIR, "pnad_mapping.json")["mapping"]

PNAD_STANDARD_RENAMES = {
    "P_ETHNICITY": "P_ETHNIC",
}

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
    "P_ETHNICITY",
    "P_AGE",
    "P_EDUCATION"
]

def main():
    PNAD_data_dom = {
        2001: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2001",
        2002: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2002",
        2003: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2003",
        2004: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2004",
        2005: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2005",
        2006: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2006",
        2007: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2007",
        2008: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2008",
        2009: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2009",
        2011: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\dom\2011"
    }

    PNAD_data_pes = {
        2001: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2001",
        2002: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2002",
        2003: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2003",
        2004: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2004",
        2005: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2005",
        2006: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2006",
        2007: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2007",
        2008: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2008",
        2009: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2009",
        2011: r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\pes\2011"
    }

    all_grouped_dfs = []
    for year, path in PNAD_data_dom.items():

        print(f"{year}: {path}")

        raw_dic_dom = pd.read_excel(r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\raw_pnad_dict.xlsx", sheet_name=f"dom{year}")
        dic_dom=harmonizer_utils.s4h_standardize_dict(raw_dic_dom)
        colnames_dom, colspecs_dom = extractor_utils.s4h_parse_fwf_dict(dic_dom)
        
        extractor_dom = Extractor(
            input_path=path,
            is_fwf=True,
            down_ext=['.txt'],
            output_path=OUTPUT_PATH,
            colnames=colnames_dom,
            colspecs=colspecs_dom
        )
        dfs_extracted = extractor_dom.s4h_extract()
        for df in dfs_extracted:
            df['YEAR'] = year

        har = Harmonizer()
        dfs = har.s4h_vertical_merge(dfs_extracted, overlap_threshold=0.9, method="union")
        dfs = merge_factor(dfs, factor_col='V4611', id_col='V0102')

        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)

        print(f"\nAplicando mapeos de valores para año {year}...")
        dfs = apply_value_mappings(dfs, year, PNAD_VALUE_MAPPING, column_aliases=COLUMN_MAPPING_BY_YEAR)

        for i, df in enumerate(dfs):
            rename_map = {old: new for old, new in PNAD_STANDARD_RENAMES.items() if old in df.columns}
            if rename_map:
                dfs[i] = df.rename(columns=rename_map)

        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]

        for extra_col in fallback_col_cols:
            if any(extra_col in df.columns for df in dfs) and extra_col not in available_harmonized:
                available_harmonized.append(extra_col)

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
    for year, path in PNAD_data_pes.items():

        print(f"{year}: {path}")

        raw_dic_pes = pd.read_excel(r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\raw_pnad_dict.xlsx", sheet_name=f"pes{year}")

        dic_pes=harmonizer_utils.s4h_standardize_dict(raw_dic_pes)
        colnames_pes, colspecs_pes = extractor_utils.s4h_parse_fwf_dict(dic_pes)

        extractor_pes = Extractor(
            input_path=path,
            is_fwf=True,
            down_ext=['.txt'],
            output_path=OUTPUT_PATH,
            colnames=colnames_pes,
            colspecs=colspecs_pes
        )
        dfs_extracted = extractor_pes.s4h_extract()
        for df in dfs_extracted:
            df['YEAR'] = year

        har = Harmonizer()
        dfs = har.s4h_vertical_merge(dfs_extracted, overlap_threshold=0.9, method="union")
        dfs = merge_factor(dfs, factor_col='V4729', id_col='V0102')

        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)

        print(f"\nAplicando mapeos de valores para año {year}...")
        dfs = apply_value_mappings(dfs, year, PNAD_VALUE_MAPPING, column_aliases=COLUMN_MAPPING_BY_YEAR)

        for i, df in enumerate(dfs):
            rename_map = {old: new for old, new in PNAD_STANDARD_RENAMES.items() if old in df.columns}
            if rename_map:
                dfs[i] = df.rename(columns=rename_map)

        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]

        for extra_col in fallback_col_cols:
            if any(extra_col in df.columns for df in dfs) and extra_col not in available_harmonized:
                available_harmonized.append(extra_col)

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
        print("\nCombining and aligning PNAD data horizontally...")

        combined_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df = combined_df.groupby(['YEAR', 'ADMIN_DIVISION'], as_index=False, dropna=False).first()
        final_df = final_df.sort_values(by=['YEAR', 'ADMIN_DIVISION']).reset_index(drop=True)

        ordered_cols = ['YEAR', 'ADMIN_DIVISION'] + [col for col in final_df.columns if col not in ['YEAR', 'ADMIN_DIVISION']]
        final_df = final_df[ordered_cols]

        output_file = f"{OUTPUT_PATH}\\PNAD_harmonized.csv"
        final_df.to_csv(output_file, index=False)
        print(f"Successfully saved final harmonized data to {output_file}")
    else:
        print("No data to save.")


if __name__ == "__main__":
    main()

