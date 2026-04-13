
from socio4health import Harmonizer
from harmonize_utils import extract_and_prepare_data, merge_factor, select_and_filter_columns, group_and_onehot_encode
import pandas as pd


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\OUTPUT"


# Diccionario de columnas por año
col_cols_dict = {
    2006: [
        "YEAR", "HVIVIEN", "HPROVI", "HZONA", "V101", "V102", "V104", "V103", "V116", "V114", "V119", "V110", "V108", "V105", "P301", "P202", "P203", "P303", "HHWEIGHT"
    ],
    # Ejemplo para otros años:
    # 2007: ["YEAR", "OTRA_COL1", "OTRA_COL2", ...],
}


def main():
    ENHOGAR_data = {
        2006: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2006",
        #2007: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2007",
        #2008: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2008",
        #2010: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2010",
        #2011: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2011",
        #2012: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2012",
        #2013: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2013",
        #2014: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2014",
        #2015: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2015",
        #2016: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2016",
        #2017: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2017",
        #2018: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2018",
        #2019: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2019",
        #2021: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2021",
        #2022: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2022",
        #2024: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2024"
    }
    all_grouped_dfs = []
    for year, path in ENHOGAR_data.items():
        ddfs = extract_and_prepare_data(year, path, ext='.csv', sep=',', output_path=OUTPUT_PATH)
        har = Harmonizer()
        dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.9, method="union")
        print(f"Number of DataFrames before column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        dfs = merge_factor(dfs, factor_col='HHWEIGHT', id_col=None)
        # Selección dinámica de columnas según el año
        col_cols = col_cols_dict.get(year, [])
        dfs = select_and_filter_columns(dfs, col_cols, num_cols_threshold=0)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        grouped_dfs = group_and_onehot_encode(dfs, group_col='HPROVI', weight_col='HHWEIGHT', id_col=None)
        all_grouped_dfs.extend(grouped_dfs)
    if all_grouped_dfs:
        final_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/ENHOGAR_harmonized.csv", index=False)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()

