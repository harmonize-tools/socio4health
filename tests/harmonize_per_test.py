
from socio4health import Harmonizer
from harmonize_utils import extract_and_prepare_data, merge_factor, select_and_filter_columns, group_and_onehot_encode
import pandas as pd


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\OUTPUT"

col_cols = [
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
        dfs = select_and_filter_columns(dfs, col_cols, num_cols_threshold=7)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        grouped_dfs = group_and_onehot_encode(dfs, group_col='UBIGEO', weight_col='FACTOR07', id_col='VIVIENDA')
        all_grouped_dfs.extend(grouped_dfs)
    if all_grouped_dfs:
        final_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/ENAHO_harmonized.csv", index=False)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
        

