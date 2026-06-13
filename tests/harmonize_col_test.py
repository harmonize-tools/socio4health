
from pathlib import Path
import re

from socio4health import Harmonizer
from socio4health.utils.harmonizer_utils import apply_value_mappings, extract_and_prepare_data, harmonize_columns_by_year, merge_factor, select_and_filter_columns, group_and_onehot_encode, _clean_column_name
import pandas as pd

from socio4health.utils.mapping_utils import load_json_mapping, load_int_key_mapping


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\OUTPUT"

_DATA_DIR = Path(__file__).resolve().parent / "col_mapping"
COLUMN_MAPPING_BY_YEAR = load_json_mapping(_DATA_DIR, "column_mapping.json")["2007-2025"]
HARMONIZED_MAPPING = load_int_key_mapping(Path(__file__).resolve().parent, "harmonized_mapping.json")
GEIH_VALUE_MAPPING = load_int_key_mapping(_DATA_DIR, "geih_mapping.json")["mapping"]

SPECIAL_CLASE_FILE_PATTERN = re.compile(r".+\*csv_.+ - .+\.csv$", re.IGNORECASE)

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


def add_clase_to_special_csvs(folder_path):
    folder = Path(folder_path)
    
    for csv_path in folder.rglob("*.csv"):
        if not SPECIAL_CLASE_FILE_PATTERN.match(csv_path.name):
            continue

        df = pd.read_csv(csv_path, sep=';', encoding='latin-1')
        header_columns = [_clean_column_name(col) for col in df.columns]
        
        if 'CLASE' not in header_columns:
            df['CLASE'] = 1
            df.to_csv(csv_path, sep=';', index=False, encoding='latin-1')
            print(f"Added CLASE=1 to {csv_path}")

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
        add_clase_to_special_csvs(path)
        ddfs = extract_and_prepare_data(year, path, ext='.csv', sep=';', output_path=OUTPUT_PATH, on_bad_lines='skip')
        ddfs = harmonize_directorio_columns(ddfs)
        ddfs = merge_factor(ddfs, factor_col='FEX_C18', id_col='DIRECTORIO')
        har = Harmonizer()
        dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.9, method="union")

        # Harmonizar nombres de columnas
        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)

        # Aplicar mapeos de valores
        print(f"\nAplicando mapeos de valores para año {year}...")
        dfs = apply_value_mappings(dfs, year, GEIH_VALUE_MAPPING, column_aliases=COLUMN_MAPPING_BY_YEAR)

        # Seleccionar las columnas harmonizadas inferidas y conservar metadatos necesarios para el agrupado
        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]

        for required_col in ("YEAR", "ADMIN_DIVISION", "EXP_FACTOR"):
            if any(required_col in df.columns for df in dfs) and required_col not in available_harmonized:
                available_harmonized.append(required_col)

        dfs = select_and_filter_columns(dfs, available_harmonized, num_cols_threshold=5)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
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
        final_df = pd.concat(all_grouped_dfs, ignore_index=True)
        if 'YEAR' in final_df.columns:
            ordered_cols = ['YEAR'] + [col for col in final_df.columns if col != 'YEAR']
            final_df = final_df[ordered_cols]
        final_df.to_csv(f"{OUTPUT_PATH}/GEIH_harmonized.csv", index=False)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()

