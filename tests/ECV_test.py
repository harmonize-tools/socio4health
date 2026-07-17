from socio4health import Extractor, Harmonizer
from functools import reduce
import pandas as pd
from pathlib import Path
import uuid


if __name__ == "__main__":
    ecv_data = {
        2010: "https://microdatos.dane.gov.co/index.php/catalog/201/get-microdata",
        2011: "https://microdatos.dane.gov.co/index.php/catalog/196/get-microdata",
        2012: "https://microdatos.dane.gov.co/index.php/catalog/124/get-microdata",
        2013: "https://microdatos.dane.gov.co/index.php/catalog/213/get-microdata",
        2014: "https://microdatos.dane.gov.co/index.php/catalog/342/get-microdata",
        2015: "https://microdatos.dane.gov.co/index.php/catalog/419/get-microdata",
        2016: "https://microdatos.dane.gov.co/index.php/catalog/456/get-microdata",
        2017: "https://microdatos.dane.gov.co/index.php/catalog/544/get-microdata"
    }

    vivienda_cols = [
        "DIRECTORIO", "YEAR", "CLASE", "REGION", "P1_DEPARTAMENTO", "P1_MUNICIPIO",
        "P4000", "P4005", "P4015", "P8520S1", "P8520S3", "P8520S4", "P8520S5", "P70", "CANT_HOGARES_VIVIENDA"
    ]
    
    hogar_cols = [
        "DIRECTORIO", "YEAR", "CLASE", "REGION", "P1_DEPARTAMENTO", "P1_MUNICIPIO",
        "P205", "CANT_PERSONAS_HOGAR", "P5010", "P8526", "P8530", "P8532", "P764"
    ]

    dfs_vivienda_by_year = {}
    dfs_hogar_by_year = {}
    
    for year, url in ecv_data.items():
        print(f"\n{year}: {url}")
        
        # Keywords para cada año
        if year == 2010:
            key_words = [
                r"(?i)datos[\s_]+(?:de[\s_]+)?identificaci[oó]n",
                r"(?i)servicios[\s_]+(?:del[\s_]+)?hogar",
                r"(?i)composici[oó]n[\s_]+(?:del[\s_]+)?hogar",
                r"(?i)datos[\s_]+(?:de[\s_]+)?(?:la[\s_]+)?vivienda",
            ]
        elif year == 2011:
            key_words = [
                r"(?i)identificaci[oó]n.*vivienda",
                r"(?i)vivienda.*identificaci[oó]n",
                r"(?i)datos[\s_]+de[\s_]+identificaci[oó]n",
                r"(?i)datos[\s_]+de[\s_]+la[\s_]+vivienda",
                r"(?i)servicios[\s_]+(?:del[\s_]+)?hogar",
            ]
        else:
            key_words = [
                r"(?i)datos[\s_]+(?:de[\s_]+)?identificaci[oó]n",
                r"(?i)servicios[\s_]+(?:del[\s_]+)?hogar",
                r"(?i)composici[oó]n[\s_]+(?:del[\s_]+)?hogar",
                r"(?i)datos[\s_]+(?:de[\s_]+)?(?:la[\s_]+)?vivienda",
            ]
        
        extractor = Extractor(
            input_path=url,
            down_ext=['.sav', '.zip'],
            sep=' ',
            output_path = f"data/ECV/ECV_{year}",
            depth=0,
            key_words=key_words,
            delete_zip_after=True
        )

        df_extracted = extractor.s4h_extract()
        
        dfs_vivienda_year = []
        dfs_hogar_year = []
        
        for df in df_extracted:
            df.columns = df.columns.str.strip()
            df.columns = [col.upper() for col in df.columns]
            df['YEAR'] = year
            
            # Normalizar nombres
            rename_map = {}
            if 'CANT_HOGARES_VIVIENDA' not in df.columns and 'CANT_HOG_COMPLETOS' in df.columns:
                rename_map['CANT_HOG_COMPLETOS'] = 'CANT_HOGARES_VIVIENDA'
            if rename_map:
                df = df.rename(columns=rename_map)
            
            # Renombrar columnas
            if 'DPTO' in df.columns and 'P1_DEPARTAMENTO' not in df.columns:
                df = df.rename(columns={'DPTO': 'P1_DEPARTAMENTO'})
            if 'P3' in df.columns and 'CLASE' not in df.columns:
                df = df.rename(columns={'P3': 'CLASE'})
            
            # Determinar si es tabla de vivienda o hogar
            has_vivienda_vars = any(col in df.columns for col in ['P4000', 'P4005', 'P4015', 'P8520'])
            has_hogar_vars = any(col in df.columns for col in ['P205', 'P5010', 'P8526', 'P8530', 'P8532', 'CANT_PERSONAS_HOGAR'])
            
            if 'DIRECTORIO' not in df.columns or df['DIRECTORIO'].isna().all():
                continue
            
            if has_vivienda_vars and not has_hogar_vars:
                dfs_vivienda_year.append(df)
            elif has_hogar_vars:
                dfs_hogar_year.append(df)
            elif has_vivienda_vars and has_hogar_vars:
                dfs_vivienda_year.append(df)
        
        # Merge de tablas de vivienda del mismo año
        if dfs_vivienda_year:
            if len(dfs_vivienda_year) == 1:
                merged_vivienda = dfs_vivienda_year[0]
            else:
                common_cols = set(dfs_vivienda_year[0].columns)
                for df in dfs_vivienda_year[1:]:
                    common_cols = common_cols.intersection(set(df.columns))
                common_cols.discard('DIRECTORIO')
                
                for i in range(1, len(dfs_vivienda_year)):
                    cols_to_drop = [col for col in common_cols if col in dfs_vivienda_year[i].columns]
                    if cols_to_drop:
                        dfs_vivienda_year[i] = dfs_vivienda_year[i].drop(columns=cols_to_drop)
                
                merged_vivienda = reduce(
                    lambda left, right: pd.merge(left, right, on='DIRECTORIO', how='outer'), 
                    dfs_vivienda_year
                )
            
            # Filtrar solo columnas de vivienda
            merged_vivienda = merged_vivienda[[col for col in vivienda_cols if col in merged_vivienda.columns]]
            dfs_vivienda_by_year[year] = merged_vivienda
            print(f"  -> Vivienda {year}: {len(merged_vivienda)} viviendas, columnas: {list(merged_vivienda.columns)}")
        
        # Merge de tablas de hogar del mismo año
        if dfs_hogar_year:
            if len(dfs_hogar_year) == 1:
                merged_hogar = dfs_hogar_year[0]
            else:
                common_cols = set(dfs_hogar_year[0].columns)
                for df in dfs_hogar_year[1:]:
                    common_cols = common_cols.intersection(set(df.columns))
                common_cols.discard('DIRECTORIO')
                
                for i in range(1, len(dfs_hogar_year)):
                    cols_to_drop = [col for col in common_cols if col in dfs_hogar_year[i].columns]
                    if cols_to_drop:
                        dfs_hogar_year[i] = dfs_hogar_year[i].drop(columns=cols_to_drop)
                
                merged_hogar = reduce(
                    lambda left, right: pd.merge(left, right, on='DIRECTORIO', how='outer'), 
                    dfs_hogar_year
                )
            
            # Filtrar solo columnas de hogar
            merged_hogar = merged_hogar[[col for col in hogar_cols if col in merged_hogar.columns]]
            dfs_hogar_by_year[year] = merged_hogar
            print(f"  -> Hogar {year}: {len(merged_hogar)} hogares, columnas: {list(merged_hogar.columns)}")
    
    # Extraer factores de expansion por año
    dfs_fex_by_year = {}
    for year, url in ecv_data.items():
        if 2010 <= year <= 2018:
            print(f"\n{year}: Extrayendo factores de expansion...")
            key_words_fex_2018 = [
                r"(?i)factores[\s_]+de[\s_]+expansi[oó]n[\s_]+(?:20)?[0-9]{2}[\s_]+basados[\s_]+en[\s_]+el[\s_]+CNPV[\s_]+2018",
            ]

            extractor_fex_2018 = Extractor(
                input_path=url,
                down_ext=['.sav', '.zip'],
                sep=' ',
                output_path = f"data/ECV/ECV_FEX_2018_{year}",
                depth=0,
                key_words=key_words_fex_2018,
                delete_zip_after=True
            )

            try:
                df_extracted_fex_2018 = extractor_fex_2018.s4h_extract()
                if df_extracted_fex_2018:
                    df_fex = df_extracted_fex_2018[0]
                    df_fex.columns = df_fex.columns.str.strip()
                    df_fex.columns = [col.upper() for col in df_fex.columns]
                    
                    if 'DIRECTORIO' in df_fex.columns and 'FEX_C_2018' in df_fex.columns:
                        df_fex = df_fex[['DIRECTORIO', 'FEX_C_2018']].drop_duplicates(subset=['DIRECTORIO'])
                        dfs_fex_by_year[year] = df_fex
                        print(f"  -> FEX {year}: {len(df_fex)} registros")
            except Exception as e:
                print(f"Skipping year {year} for FEX_C_2018 extraction: {e}")

    # Extraer municipio por año
    dfs_mpio_by_year = {}
    for year, url in ecv_data.items():
        print(f"\n{year}: Extrayendo municipio...")
        extractor_mpio = Extractor(
            input_path=url,
            down_ext=['.sav', '.zip'],
            sep=' ',
            output_path = f"data/ECV/ECV_MPIO_{year}",
            depth=0,
            key_words=[
                r"(?i)municipio[\s_]+de[\s_]+aplicaci[oó]n[\s_]+de[\s_]+la[\s_]+encuesta[\s_]+(?:20)?[0-9]{2}",
            ],
            delete_zip_after=True
        )

        try:
            df_extracted_mpio = extractor_mpio.s4h_extract()
            if df_extracted_mpio:
                df_mpio = df_extracted_mpio[0]
                df_mpio.columns = df_mpio.columns.str.strip()
                df_mpio.columns = [col.upper() for col in df_mpio.columns]
                
                if 'DIRECTORIO' in df_mpio.columns and 'P1_MUNICIPIO' in df_mpio.columns:
                    df_mpio = df_mpio[['DIRECTORIO', 'P1_MUNICIPIO']].drop_duplicates(subset=['DIRECTORIO'])
                    dfs_mpio_by_year[year] = df_mpio
                    print(f"  -> Municipio {year}: {len(df_mpio)} registros")
        except Exception as e:
            print(f"Skipping year {year} for municipio extraction: {e}")

    # Extraer identificacion 2010
    dfs_2010_id = []
    for year, url in ecv_data.items():
        if 2010 == year:
            print(f"\n{year}: Extrayendo identificacion-final...")
            key_words_2010_id = ["Datos de idenrificacion-final"]
            extractor_2010_id = Extractor(
                input_path=url,
                down_ext=['.sav', '.zip'],
                sep=' ',
                output_path = f"data/ECV/ECV_2010_ID_{year}",
                depth=0,
                key_words=key_words_2010_id,
                delete_zip_after=True
            )

            try:
                dfs_2010_id = extractor_2010_id.s4h_extract()
                for df in dfs_2010_id:
                    df.columns = df.columns.str.strip()
                    df.columns = [col.upper() for col in df.columns]
            except Exception as e:
                print(f"Skipping year {year} for 2010_ID extraction: {e}")

    # Procesar cada año por separado
    for year in ecv_data.keys():
        print(f"\nProcesando {year}...")
        
        # Merge con factores de expansion (solo en df_hogar)
        if year in dfs_hogar_by_year and year in dfs_fex_by_year:
            df_hogar = dfs_hogar_by_year[year]
            df_fex = dfs_fex_by_year[year]
            
            df_hogar = df_hogar.merge(df_fex, on='DIRECTORIO', how='left')
            dfs_hogar_by_year[year] = df_hogar
            print(f"  -> Merge FEX_C_2018 completado en df_hogar")
        
        # Merge con municipio
        if year in dfs_vivienda_by_year and year in dfs_mpio_by_year:
            df_vivienda = dfs_vivienda_by_year[year]
            df_mpio = dfs_mpio_by_year[year]
            
            if 'P1_MUNICIPIO' not in df_vivienda.columns or df_vivienda['P1_MUNICIPIO'].isna().all():
                df_vivienda = df_vivienda.merge(df_mpio, on='DIRECTORIO', how='left')
                dfs_vivienda_by_year[year] = df_vivienda
                print(f"  -> Merge P1_MUNICIPIO completado")
        
        # Merge con identificacion 2010 (solo para 2010)
        if year == 2010 and dfs_2010_id:
            df_2010_id = dfs_2010_id[0]
            
            if 'P3' in df_2010_id.columns and 'CLASE' not in df_2010_id.columns:
                df_2010_id = df_2010_id.rename(columns={'P3': 'CLASE'})
            
            rename_map = {}
            if 'P1_DEPARTAMENTO' not in df_2010_id.columns and 'DEPARTAMENTO' in df_2010_id.columns:
                rename_map['DEPARTAMENTO'] = 'P1_DEPARTAMENTO'
            if rename_map:
                df_2010_id = df_2010_id.rename(columns=rename_map)
            
            if 'DIRECTORIO' in df_2010_id.columns:
                df_2010_id = df_2010_id.drop_duplicates(subset=['DIRECTORIO'], keep='first')
                
                cols_to_merge = ['DIRECTORIO']
                for col in ['P1_DEPARTAMENTO', 'CLASE', 'REGION']:
                    if col in df_2010_id.columns:
                        cols_to_merge.append(col)
                
                if len(cols_to_merge) > 1 and year in dfs_vivienda_by_year:
                    df_vivienda = dfs_vivienda_by_year[year]
                    
                    df_vivienda = df_vivienda.merge(
                        df_2010_id[cols_to_merge], 
                        on='DIRECTORIO', 
                        how='left', 
                        suffixes=('', '_2010')
                    )
                    
                    for col in ['P1_DEPARTAMENTO', 'CLASE', 'REGION']:
                        col_2010 = f'{col}_2010'
                        if col_2010 in df_vivienda.columns:
                            if col in df_vivienda.columns:
                                df_vivienda[col] = df_vivienda[col].combine_first(df_vivienda[col_2010])
                            else:
                                df_vivienda[col] = df_vivienda[col_2010]
                            df_vivienda = df_vivienda.drop(columns=[col_2010])
                    
                    dfs_vivienda_by_year[year] = df_vivienda
                    print(f"  -> Merge identificacion 2010 completado")
    
    # Guardar un único archivo por año con las columnas de vivienda y hogar
    run_dir = Path("data/dfs") / f"run_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=False)
    
    print(f"\nGuardando archivos en: {run_dir}")
    
    # Función helper para convertir tipos: todo a Int64 excepto FEX_C_2018
    def convert_dtypes(df):
        for col in df.columns:
            if col == 'FEX_C_2018':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        return df
    
    years = sorted(set(dfs_vivienda_by_year.keys()) | set(dfs_hogar_by_year.keys()))
    for year in years:
        df_vivienda = dfs_vivienda_by_year.get(year)
        df_hogar = dfs_hogar_by_year.get(year)

        if df_vivienda is not None and df_hogar is not None:
            df_joined = df_vivienda.merge(
                df_hogar,
                on='DIRECTORIO',
                how='outer',
                suffixes=('_vivienda', '_hogar')
            )

            if 'YEAR_vivienda' in df_joined.columns:
                df_joined = df_joined.rename(columns={'YEAR_vivienda': 'YEAR'})
            if 'YEAR_hogar' in df_joined.columns:
                if 'YEAR' in df_joined.columns:
                    df_joined['YEAR'] = df_joined['YEAR'].combine_first(df_joined['YEAR_hogar'])
                else:
                    df_joined = df_joined.rename(columns={'YEAR_hogar': 'YEAR'})
                df_joined = df_joined.drop(columns=['YEAR_hogar'])
        elif df_vivienda is not None:
            df_joined = df_vivienda.copy()
        elif df_hogar is not None:
            df_joined = df_hogar.copy()
        else:
            continue

        df_joined = convert_dtypes(df_joined)

        output_file = run_dir / f"df_ecv_{year}.csv"
        df_joined.to_csv(output_file, index=False)
        print(f"  -> {year}: {len(df_joined)} filas guardadas en {output_file.name}")
    
    print(f"\nProceso completado. Archivos disponibles en: {run_dir}")

    if extractor is not None:
        extractor.s4h_delete_download_folder(folder_path="data/ECV")