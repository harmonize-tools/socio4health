
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
    HARMONIZED_MAPPING,
    get_value_mapping_path,
    get_columns_for_year,
)
import pandas as pd
from functools import reduce
import json


OUTPUT_PATH = r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\OUTPUT"


def main():
    ENHOGAR_data = {
        2006: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2006",
        2007: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2007",
        2008: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2008",
        2010: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2010",
        2011: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2011",
        2012: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2012",
        2013: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2013",
        2014: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2014",
        2015: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2015",
        2016: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2016",
        2017: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2017",
        2018: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2018",
        2019: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2019",
        2021: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2021",
        2022: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2022",
        2024: r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\2024"
    }


    final_dfs = []
    for year, path in ENHOGAR_data.items():
        print(f"\n{'='*60}")
        print(f"Procesando año {year}")
        print(f"{'='*60}")
        
        # Extraer datos
        ddfs = extract_and_prepare_data(year, path, ext='.csv', sep=',', output_path=OUTPUT_PATH)
        dfs = [df.compute() for df in ddfs]
        
        # Selección de columnas originales según el año
        col_cols = get_columns_for_year(year)
        dfs = select_and_filter_columns(dfs, col_cols, num_cols_threshold=0)
        
        # Harmonizar nombres de columnas
        print(f"\nArmonizando nombres de columnas para año {year}...")
        dfs = harmonize_columns_by_year(dfs, year, COLUMN_MAPPING_BY_YEAR)
        
        # Aplicar mapeos de valores
        print(f"\nAplicando mapeos de valores para año {year}...")
        dfs = apply_value_mappings(dfs, year, get_value_mapping_path(year))
        
        # Seleccionar las columnas harmonizadas inferidas y conservar metadatos necesarios para el agrupado
        available_harmonized = [
            col for col in HARMONIZED_MAPPING.keys()
            if any(col in df.columns for df in dfs)
        ]

        if(year == 2006):
            with open("tests/rd_year_mappings/region_mapping.json", "r", encoding="utf-8") as f:
                region_map = json.load(f)
            region_map = {str(k): v for k, v in region_map.items()}

            for df in dfs:
                if "ADMIN_DIVISION_2" in df.columns and "ADMIN_DIVISION" not in df.columns:
                    df["ADMIN_DIVISION"] = df["ADMIN_DIVISION_2"].astype(str).map(region_map)

        for required_col in ("ADMIN_DIVISION", "EXP_FACTOR"):
            if any(required_col in df.columns for df in dfs) and required_col not in available_harmonized:
                available_harmonized.append(required_col)

        dfs = select_and_filter_columns(dfs, available_harmonized, num_cols_threshold=0)
        
        print(f"\nColumns after harmonization: {available_harmonized}")
        
        grouped_dfs = group_and_onehot_encode(dfs, group_col='ADMIN_DIVISION', weight_col='EXP_FACTOR', id_col=None)
    
        if grouped_dfs:
            # 1. Set the join key as the index for all DataFrames
            indexed_dfs = [df.set_index('ADMIN_DIVISION') for df in grouped_dfs]
            
            # 2. Start with the first DataFrame as our base
            merged_df = indexed_dfs[0]
            
            # 3. Sequentially merge horizontally.
            for df in indexed_dfs[1:]:
                merged_df = merged_df.combine_first(df)
                
            # 4. Reset index to make 'ADMIN_DIVISION' a regular column again
            merged_df = merged_df.reset_index()
            merged_df['YEAR'] = year
            final_dfs.append(merged_df)
        else:
            print("No data to save.")

    final_df = pd.concat(final_dfs, ignore_index=True)
    
    fixed_cols = ['YEAR', 'ADMIN_DIVISION']
    data_cols = sorted([col for col in final_df.columns if col not in fixed_cols])
    final_df = final_df[fixed_cols + data_cols]
    #final_df = final_df.sort_values(by=['ADMIN_DIVISION', 'YEAR']).reset_index(drop=True)

    final_df.to_csv(f"{OUTPUT_PATH}/ENHOGAR_harmonized.csv", index=False)
    print(f"\nFinal harmonized data saved to {OUTPUT_PATH}/ENHOGAR_harmonized.csv")
    print(f"Final shape: {final_df.shape}")
    

if __name__ == "__main__":
    main()

