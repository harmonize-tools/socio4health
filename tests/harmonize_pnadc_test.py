from socio4health.utils import extractor_utils, harmonizer_utils
from socio4health import Extractor, Harmonizer
from harmonize_utils import extract_and_prepare_data, merge_factor, select_and_filter_columns, group_and_onehot_encode
import pandas as pd
import os


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\OUTPUT"

cols = [
        "UPA",
        "CAPITAL",
        "V1032",
        "V1022",
        "S01001",
        "S01002",
        "S01003",
        "S01004",
        "S01014",
        "S01012A",
        "S01013",
        "S01007",
        "S01016B",
        "S01017",
        "V3001",
        "V2007",
        "V2010",
        "V2009",
        "V3003A"
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


    

    all_grouped_dfs_dom = []
    
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
        #for df in dfs:
        #    print(df.head())
        #dfs = merge_factor(dfs, factor_col='V4611', id_col='V0102')
        dfs = select_and_filter_columns(dfs, cols, num_cols_threshold=0)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        grouped_dfs = group_and_onehot_encode(dfs, group_col='CAPITAL', weight_col='V1032', id_col='UPA')
        all_grouped_dfs_dom.extend(grouped_dfs)
    if all_grouped_dfs_dom:
        final_df = pd.concat(all_grouped_dfs_dom, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/PNADC_harmonized.csv", index=False)
    else:
        print("No data to save.")
    

if __name__ == "__main__":
    main()

