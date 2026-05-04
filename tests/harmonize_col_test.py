
from socio4health import Harmonizer
from harmonize_utils import extract_and_prepare_data, merge_factor, select_and_filter_columns, group_and_onehot_encode, _clean_column_name
import pandas as pd


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\OUTPUT"

col_cols = [
        "YEAR",
        "DIRECTORIO",
        "DPTO",
        "FEX_C18",
        "CLASE",
        "P4000",
        "P4005",
        "P4010",
        "P4020",
        "P4030S1",
        "P5020",
        "P5040",
        "P5050",
        "P5080",
        "P5090",
        "P5000",
        "P5010",
        "P6160",
        "P3271",
        "P6080",
        "P6040",
        "P6042",
        "Ï»¿DIRECTORIO",
        "P6020",
        "P6210"
    ]


def harmonize_directorio_columns(dfs):
    for i, df in enumerate(dfs):
        rename_map = {}
        for col in df.columns:
            if _clean_column_name(col) == 'DIRECTORIO' and col != 'DIRECTORIO':
                rename_map[col] = 'DIRECTORIO'
        if rename_map:
            if 'DIRECTORIO' in df.columns:
                df = df.drop(columns=['DIRECTORIO'])
            df = df.rename(columns=rename_map)
            dfs[i] = df
    return dfs

def main():
    GEIH_data = {
        2007: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2007",
        2008: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2008",
        2009: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2009",
        2010: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2010",
        2011: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2011",
        2012: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2012",
        2013: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2013",
        2014: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2014",
        2015: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2015",
        2016: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2016",
        2017: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2017",
        2018: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2018",
        2019: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2019",
        2020: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2020",
        2021: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2021",
        2022: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2022",
        2023: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2023",
        2024: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2024",
        2025: r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\2025"
    }
    all_grouped_dfs = []
    for year, path in GEIH_data.items():
        ddfs = extract_and_prepare_data(year, path, ext='.csv', sep=';', output_path=OUTPUT_PATH, on_bad_lines='skip')
        ddfs = harmonize_directorio_columns(ddfs)
        ddfs = merge_factor(ddfs, factor_col='FEX_C18', id_col='DIRECTORIO')
        har = Harmonizer()
        dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.9, method="union")
        dfs = select_and_filter_columns(dfs, col_cols, num_cols_threshold=5)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        grouped_dfs = group_and_onehot_encode(dfs, group_col='DPTO', weight_col='FEX_C18', id_col='DIRECTORIO')
        all_grouped_dfs.extend(grouped_dfs)
    if all_grouped_dfs:
        final_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/GEIH_harmonized.csv", index=False)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()

