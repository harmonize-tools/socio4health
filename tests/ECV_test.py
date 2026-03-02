from socio4health import Extractor, Harmonizer

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
    for year, url in ecv_data.items():
        print(f"{year}: {url}")
        extractor = Extractor(
            input_path=url,
            down_ext=['.sav', '.zip'],
            sep=' ',
            output_path = f"data/ECV_{year}",
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
            df['year'] = year
        ddfs.extend(df_extracted)

    for ddf in ddfs:
        print(ddf.head())

    har = Harmonizer()
    dfs = har.s4h_vertical_merge(ddfs, overlap_threshold=0.6, method="union")

    print(len(dfs))

    for i, df in enumerate(dfs):
        print(f"DF {i} columns:")
        print(len(df.columns))
        print(list(df.columns))
        print("-" * 40)