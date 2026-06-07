
from socio4health import Harmonizer
from harmonize_utils import (
    extract_and_prepare_data, 
    merge_factor, 
    select_and_filter_columns, 
    group_and_onehot_encode,
    harmonize_columns_by_year,
    apply_value_mappings
)
from rd_year_mappings import (
    COLUMN_MAPPING_BY_YEAR, 
    VALUE_MAPPING_BY_YEAR,
    HARMONIZED_MAPPING,
    get_columns_for_year,
)
import pandas as pd


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\OUTPUT"


def main():
    ENHOGAR_data = {
        #2006: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2006",
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
        2024: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2024"
    }
    all_grouped_dfs = []
    for year, path in ENHOGAR_data.items():
        print(f"\n{'='*60}")
        print(f"Procesando año {year}")
        print(f"{'='*60}")
        
        # Extraer datos
        ddfs = extract_and_prepare_data(year, path, ext='.csv', sep=',', output_path=OUTPUT_PATH)
        
        # Merge vertical
        har = Harmonizer()
        dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.9, method="union")
        print(f"Number of DataFrames before column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        
        # Merge factor
        dfs = merge_factor(dfs, factor_col='EXP_FACTOR', id_col=None)
        
        # Selección de columnas originales según el año
        col_cols = get_columns_for_year(year)
        dfs = select_and_filter_columns(dfs, col_cols, num_cols_threshold=0)
        print(f"Number of DataFrames after column selection: {len(dfs)} year: {year}")
        for i, df in enumerate(dfs):
            print(f"DataFrame {i} columns: {list(df.columns)}")
        
        # Harmonizar nombres de columnas
        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)
        
        # Aplicar mapeos de valores
        print(f"\nAplicando mapeos de valores para año {year}...")
        dfs = apply_value_mappings(dfs, year, VALUE_MAPPING_BY_YEAR)
        
        # Seleccionar las columnas harmonizadas inferidas y conservar metadatos necesarios para el agrupado
        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]
        for required_col in ("ADMIN_DIVISION", "EXP_FACTOR"):
            if any(required_col in df.columns for df in dfs) and required_col not in available_harmonized:
                available_harmonized.append(required_col)
        dfs = select_and_filter_columns(dfs, available_harmonized, num_cols_threshold=0)
        
        print(f"\nColumns after harmonization: {available_harmonized}")
        
        # Combine dataframes for the year to avoid duplicated groups, then one-hot encode
        try:
            combined_df = pd.concat(dfs, ignore_index=True)
            grouped_dfs = group_and_onehot_encode([combined_df], group_col='ADMIN_DIVISION', weight_col='EXP_FACTOR', id_col=None)
        except ValueError:
            # Fall back to processing individual dfs if concat fails
            grouped_dfs = group_and_onehot_encode(dfs, group_col='ADMIN_DIVISION', weight_col='EXP_FACTOR', id_col=None)
        all_grouped_dfs.extend(grouped_dfs)
    
    if all_grouped_dfs:
        final_df = pd.concat(all_grouped_dfs, ignore_index=True)
        final_df.to_csv(f"{OUTPUT_PATH}/ENHOGAR_harmonized.csv", index=False)
        print(f"\nFinal harmonized data saved to {OUTPUT_PATH}/ENHOGAR_harmonized.csv")
        print(f"Final shape: {final_df.shape}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()

