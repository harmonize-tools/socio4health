from socio4health.utils import extractor_utils, harmonizer_utils
from socio4health import Extractor, Harmonizer
from harmonize_utils import extract_and_prepare_data, merge_factor, select_and_filter_columns, group_and_onehot_encode
import pandas as pd
import os


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\OUTPUT"

cols = [
            "V0102",
            "UF",
            "V4611",
            "V4729",
            "V4105",
            "V0202",
            "V0203",
            "V0204",
            "V0219",
            "V0217",
            "V0218",
            "V0212",
            "V0223",
            "V0207",
            "V0601",
            "V0302",
            "V0404",
            "V8005",
            "V6007"
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


    

    all_grouped_dfs_dom = []
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
        for df in dfs_extracted:
            print(f"Extracted DataFrame columns: {list(df.columns)}")

        har = Harmonizer()
        dfs = har.s4h_vertical_merge(dfs_extracted, overlap_threshold=0.9, method="union")
        dfs = merge_factor(dfs, factor_col='V4611', id_col='V0102')
        dfs = select_and_filter_columns(dfs, cols, num_cols_threshold=0)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        grouped_dfs = group_and_onehot_encode(dfs, group_col='UF', weight_col='V4611', id_col='V0102')
        all_grouped_dfs_dom.extend(grouped_dfs)
    if all_grouped_dfs_dom:
        final_df = pd.concat(all_grouped_dfs_dom, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/PNAD_harmonized_dom.csv", index=False)
    else:
        print("No data to save.")




    all_grouped_dfs_pes = []
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
        for df in dfs_extracted:
            print(f"Extracted DataFrame columns: {list(df.columns)}")

        har = Harmonizer()
        dfs = har.s4h_vertical_merge(dfs_extracted, overlap_threshold=0.9, method="union")
        dfs = merge_factor(dfs, factor_col='V4729', id_col='V0102')
        dfs = select_and_filter_columns(dfs, cols, num_cols_threshold=0)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        grouped_dfs = group_and_onehot_encode(dfs, group_col='UF', weight_col='V4729', id_col='V0102')
        all_grouped_dfs_pes.extend(grouped_dfs)
    if all_grouped_dfs_pes:
        final_df = pd.concat(all_grouped_dfs_pes, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/PNAD_harmonized_pes.csv", index=False)
    else:
        print("No data to save.")
    

if __name__ == "__main__":
    main()

