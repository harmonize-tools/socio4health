from socio4health import Extractor, Harmonizer
from functools import reduce
import pandas as pd

if __name__ == "__main__":
    ecv_data = {
        2010: "https://microdatos.dane.gov.co/index.php/catalog/201/get-microdata",
        2011: "https://microdatos.dane.gov.co/index.php/catalog/196/get-microdata",
        2012: "https://microdatos.dane.gov.co/index.php/catalog/124/get-microdata",
        2013: "https://microdatos.dane.gov.co/index.php/catalog/213/get-microdata",
        2014: "https://microdatos.dane.gov.co/index.php/catalog/342/get-microdata",
        2015: "https://microdatos.dane.gov.co/index.php/catalog/419/get-microdata",
        2016: "https://microdatos.dane.gov.co/index.php/catalog/456/get-microdata",
        2017: "https://microdatos.dane.gov.co/index.php/catalog/544/get-microdata",
        2018: "https://microdatos.dane.gov.co/index.php/catalog/607/get-microdata",
        2019: "https://microdatos.dane.gov.co/index.php/catalog/678/get-microdata",
        2020: "https://microdatos.dane.gov.co/index.php/catalog/718/get-microdata",
        2021: "https://microdatos.dane.gov.co/index.php/catalog/734/get-microdata",
        2022: "https://microdatos.dane.gov.co/index.php/catalog/793/get-microdata",
        2023: "https://microdatos.dane.gov.co/index.php/catalog/827/get-microdata",
        2024: "https://microdatos.dane.gov.co/index.php/catalog/861/get-microdata"
    }

    ddfs = []
    extractor = None
    for year, url in ecv_data.items():
        print(f"{year}: {url}")
        extractor = Extractor(
            input_path=url,
            down_ext=['.sav', '.zip'],
            sep=' ',
            output_path = f"data/ECV/ECV_{year}",
            depth=0,
            key_words=[
                r"(?i)datos[\s_]+(?:de[\s_]+)?identificaci[oó]n",
                r"(?i)servicios[\s_]+(?:del[\s_]+)?hogar",
                r"(?i)composici[oó]n[\s_]+(?:del[\s_]+)?hogar",
                r"(?i)datos[\s_]+(?:de[\s_]+)?(?:la[\s_]+)?vivienda",
            ],
            delete_zip_after=True
        )

        df_extracted = extractor.s4h_extract()
        for df in df_extracted:
            df['YEAR'] = year
        ddfs.extend(df_extracted)
    
    ddfs_mpio = []
    for year, url in ecv_data.items():
        print(f"{year}: {url}")
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
            ddfs_mpio.extend(df_extracted_mpio)
        except Exception as e:
            print(f"Skipping year {year} for municipio extraction: {e}")

    ddfs_fex_2018 = []
    for year, url in ecv_data.items():

        if 2010 <= year <= 2018:
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
                ddfs_fex_2018.extend(df_extracted_fex_2018)
            except Exception as e:
                print(f"Skipping year {year} for FEX_C_2018 extraction: {e}")

    dfs_2010_id = []
    for year, url in ecv_data.items():
        if 2010 == year:
            key_words_2010_id = [
                "Datos de idenrificacion-final",
            ]

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
            except Exception as e:
                print(f"Skipping year {year} for 2010_ID extraction: {e}")

    # Rename 'DPTO' to 'P1_DEPARTAMENTO' if needed before vertical merge
    for i, df in enumerate(ddfs):
        if 'DPTO' in df.columns and 'P1_DEPARTAMENTO' not in df.columns:
            df = df.rename(columns={'DPTO': 'P1_DEPARTAMENTO'})
            ddfs[i] = df

    har = Harmonizer()
    dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.6, method="union")

    df_mpio = har.s4h_vertical_merge(ddfs_mpio, overlap_threshold=0.6, method="union")
    dfs_fex_2018 = har.s4h_vertical_merge(ddfs_fex_2018, overlap_threshold=0.6, method="union")

    # Map FEX_C_2018 into dfs using dfs_fex_2018 by DIRECTORIO
    if isinstance(dfs_fex_2018, list):
        df_fex_2018_ref = dfs_fex_2018[0]
    else:
        df_fex_2018_ref = dfs_fex_2018
    try:
        import dask.dataframe as dd
        if isinstance(df_fex_2018_ref, dd.DataFrame):
            df_fex_2018_ref = df_fex_2018_ref.compute()
    except ImportError:
        pass
    if 'DIRECTORIO' in df_fex_2018_ref.columns and 'FEX_C_2018' in df_fex_2018_ref.columns:
        for i, df in enumerate(dfs):
            if 'DIRECTORIO' in df.columns:
                df = df.merge(df_fex_2018_ref[['DIRECTORIO', 'FEX_C_2018']], on='DIRECTORIO', how='left', suffixes=('', '_fex2018'))
                dfs[i] = df

    '''
    for i, df in enumerate(dfs):
        print(f"DataFrame {i + 1} shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
    '''  

    cols = [
        "DIRECTORIO",
        "YEAR",
        "CANT_PERSONAS_HOGAR",
        "CLASE",
        "FEX_C",
        "FEX_C_2018",
        "P1_DEPARTAMENTO",
        "P1_MUNICIPIO",
        "P1070",
        "P205",
        "P3",
        "P4000",
        "P4005",
        "P4015",
        "P5000",
        "P5010",
        "P5022",
        "P5047",
        "P5052",
        "P5052S1",
        "P5054",
        "P5661",
        "P5666",
        "P6087",
        "P6088",
        "P8520",
        "P8525",
        "P8526",
        "P853",
        "P8530",
        "PERCAPITA",
        "REGION"
    ]

    dfs = [df[[col for col in cols if col in df.columns]] for df in dfs]

    import re
    for i, df in enumerate(dfs):
        try:
            import dask.dataframe as dd
            if isinstance(df, dd.DataFrame):
                df = df.compute()
        except ImportError:
            pass
        # Enforce dtypes
        dtype_map = {
            "DIRECTORIO": "Int64",
            "YEAR": "Int64",
            "CANT_PERSONAS_HOGAR": "Int64",
            "CLASE": "Int64",
            "FEX_C": "float64",
            "FEX_C_2018": "float64",
            "P1_DEPARTAMENTO": "Int64",
            "P1_MUNICIPIO": "Int64",
            "P1070": "Int64",
            "P205": "Int64",
            "P3": "Int64",
            "P4000": "Int64",
            "P4005": "Int64",
            "P4015": "Int64",
            "P5000": "Int64",
            "P5010": "Int64",
            "P5022": "Int64",
            "P5047": "Int64",
            "P5052": "Int64",
            "P5052S1": "Int64",
            "P5054": "Int64",
            "P5661": "Int64",
            "P5666": "Int64",
            "P6087": "Int64",
            "P6088": "Int64",
            "P8520": "Int64",
            "P8525": "Int64",
            "P8526": "Int64",
            "P853": "Int64",
            "P8530": "Int64",
            "PERCAPITA": "float64",
            "REGION": "Int64"
        }
        for col, dtype in dtype_map.items():
            if col in df.columns:
                if dtype == "string":
                    # Strip FILENAME as before
                    def strip_filename(val):
                        if pd.isnull(val):
                            return val
                        m = re.match(r'^[a-fA-F0-9]+_(.*)\.[^.]+$', val)
                        if m:
                            return m.group(1)
                        return re.sub(r'\.[^.]+$', '', val)
                    df[col] = df[col].apply(strip_filename).astype("string")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
        dfs[i] = df


    if len(dfs) > 1:
        
        # Si existe dfs_2010_id, unirlo por DIRECTORIO (asegurando mayúsculas)
        if 'dfs_2010_id' in locals() and dfs_2010_id:
            df_2010_id = dfs_2010_id[0]
            df_2010_id.columns = [col.upper() for col in df_2010_id.columns]
            if 'DIRECTORIO' in df_2010_id.columns and 'P1_DEPARTAMENTO' in df_2010_id.columns and 'P3' in df_2010_id.columns and 'REGION' in df_2010_id.columns:
                dfs[1] = dfs[1].merge(df_2010_id[['DIRECTORIO', 'P1_DEPARTAMENTO', 'P3', 'REGION']], on='DIRECTORIO', how='left', suffixes=('', '_2010'))
                # Si hay conflicto de columnas, priorizar P1_DEPARTAMENTO_2010 si existe
                if 'P1_DEPARTAMENTO_2010' in dfs[1].columns:
                    dfs[1]['P1_DEPARTAMENTO'] = dfs[1]['P1_DEPARTAMENTO'].combine_first(dfs[1]['P1_DEPARTAMENTO_2010'])
                    dfs[1] = dfs[1].drop(columns=['P1_DEPARTAMENTO_2010'])
                if 'P3_2010' in dfs[1].columns:
                    dfs[1]['P3'] = dfs[1]['P3'].combine_first(dfs[1]['P3_2010'])
                    dfs[1] = dfs[1].drop(columns=['P3_2010'])
                if 'REGION_2010' in dfs[1].columns:
                    dfs[1]['REGION'] = dfs[1]['REGION'].combine_first(dfs[1]['REGION_2010'])
                    dfs[1] = dfs[1].drop(columns=['REGION_2010'])
                # Coerce dtypes simply
                for col in ['P1_DEPARTAMENTO', 'P3', 'REGION']:
                    if col in dfs[1].columns:
                        dfs[1][col] = pd.to_numeric(dfs[1][col], errors='coerce').astype('Int64')

        df_ref = dfs[1]
        try:
            import dask.dataframe as dd
            if isinstance(df_ref, dd.DataFrame):
                df_ref = df_ref.compute()
        except ImportError:
            pass


        # Reference for P1_DEPARTAMENTO, P3, REGION: dfs[1] (df_ref)
        directorio_map_depto = df_ref.set_index('DIRECTORIO')['P1_DEPARTAMENTO'].to_dict()
        directorio_map_p3 = df_ref.set_index('DIRECTORIO')['P3'].to_dict() if 'P3' in df_ref.columns else {}
        directorio_map_region = df_ref.set_index('DIRECTORIO')['REGION'].to_dict() if 'REGION' in df_ref.columns else {}
        # Reference for P1_MUNICIPIO: df_mpio
        if isinstance(df_mpio, list):
            df_mpio_ref = df_mpio[0]
        else:
            df_mpio_ref = df_mpio
        try:
            import dask.dataframe as dd
            if isinstance(df_mpio_ref, dd.DataFrame):
                df_mpio_ref = df_mpio_ref.compute()
        except ImportError:
            pass
        directorio_map_mpio = df_mpio_ref.set_index('DIRECTORIO')['P1_MUNICIPIO'].to_dict()

        # Also get municipio mapping from dfs[1] (for 2023/2024)
        directorio_map_mpio_alt = df_ref.set_index('DIRECTORIO')['P1_MUNICIPIO'].to_dict()

        for i, df in enumerate(dfs):
            if i == 1:
                continue
            try:
                import dask.dataframe as dd
                if isinstance(df, dd.DataFrame):
                    df = df.compute()
            except ImportError:
                pass
            # Map P1_DEPARTAMENTO
            df['P1_DEPARTAMENTO'] = df['DIRECTORIO'].map(lambda x: directorio_map_depto.get(x))
            df['P1_DEPARTAMENTO'] = pd.to_numeric(df['P1_DEPARTAMENTO'], errors='coerce').astype('Int64')
            # Map P3
            if directorio_map_p3:
                df['P3'] = df['DIRECTORIO'].map(lambda x: directorio_map_p3.get(x))
                df['P3'] = pd.to_numeric(df['P3'], errors='coerce').astype('Int64')
            # Map REGION
            if directorio_map_region:
                df['REGION'] = df['DIRECTORIO'].map(lambda x: directorio_map_region.get(x))
                df['REGION'] = pd.to_numeric(df['REGION'], errors='coerce').astype('Int64')
            # Try both mappings for P1_MUNICIPIO
            def municipio_mapper(x):
                val = directorio_map_mpio.get(x)
                if pd.isnull(val) or val is None:
                    val = directorio_map_mpio_alt.get(x)
                return val
            df['P1_MUNICIPIO'] = df['DIRECTORIO'].map(municipio_mapper)
            df['P1_MUNICIPIO'] = pd.to_numeric(df['P1_MUNICIPIO'], errors='coerce').astype('Int64')
            dfs[i] = df

    
    import os
    os.makedirs("data/dfs", exist_ok=True)
    for i, df in enumerate(dfs):
        df.to_csv(f"data/dfs/df_{i}.csv", index=False)

    if extractor is not None:
        extractor.s4h_delete_download_folder(folder_path="data/ECV")
