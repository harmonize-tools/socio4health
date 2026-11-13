
<a href="https://www.harmonize-tools.org/">
    <img height="120" align="right" src="https://harmonize-tools.github.io/harmonize-logo.png" />
</a>

<a href="https://harmonize-tools.github.io/socio4health/">
    <img height="120" src="https://raw.githubusercontent.com/harmonize-tools/socio4health/main/docs/source/_static/image.png" />
</a>

# socio4health

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![PyPI](https://img.shields.io/pypi/v/socio4health)](https://pypi.org/project/socio4health/)
[![MIT
license](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/harmonize-tools/socio4health/blob/main/LICENSE.md/)
[![GitHub
contributors](https://img.shields.io/github/contributors/harmonize-tools/socio4health)](https://github.com/harmonize-tools/socio4health/graphs/contributors)
![commits](https://img.shields.io/github/commit-activity/t/harmonize-tools/socio4health)
<!-- badges: end -->

**socio4health** is a Python ETL toolkit for Latin American census and household survey microdata. It downloads files straight from national statistics offices (DANE, IBGE, INEI, ...), reads them whatever their format, turns surveys from different countries and years into one harmonized dataset, and saves it as Parquet, CSV or a DuckDB database, ready for health and climate research.

## Why socio4health?

Working with microdata from several countries usually means dealing with a different portal, file format, variable naming scheme and language for each one. Tools that exist tend to cover a single country (e.g. R packages for Brazil's PNAD) or deliver already-harmonized data that you cannot extend (IPUMS, IDB). socio4health covers the whole pipeline, in Python, across countries:

| Step | What socio4health does |
|---|---|
| **Extract** | Crawls a statistics office page and downloads the files you ask for (filtered by extension and keywords), or reads a local folder. |
| **Read** | Unpacks `.zip`, `.7z`, `.tar`, `.gz` and reads `.csv`, `.txt`, fixed-width, `.xls/.xlsx`, `.sav` (SPSS), `.parquet`, `.json` and geospatial files (`.shp`, `.geojson`, `.kml`). |
| **Scale** | Returns [Dask](https://www.dask.org/) DataFrames, so full censuses and multi-year surveys can be processed without fitting in memory. An optional Parquet cache makes re-reading large text files much faster. |
| **Understand** | Standardizes raw data dictionaries, translates questions and answers to English, and classifies each variable into a theme (e.g. *Housing*, *Business*) with a fine-tuned BERT model. |
| **Harmonize** | Stacks files from different periods (`s4h_vertical_merge`), renames columns and recodes categorical values per country from a mapping (`s4h_harmonize_dataframes`), drops empty columns, selects variables by theme and filters by geography or any key (`s4h_data_selector`), and joins tables (`s4h_join_data`). |
| **Load** | Saves the result with `s4h_export` as Parquet (optionally partitioned, e.g. by year), CSV or tables in a DuckDB database, one table per DataFrame or per country. Data larger than memory is written partition by partition. |

Built-in column specifications are included for Brazil's PNAD Contínua, and the example notebooks pull data from Colombia (DANE), Brazil (IBGE) and Peru (INEI).

## Installation

Install the full package from [PyPI](https://pypi.org/project/socio4health/) (includes web scraping, machine learning and geospatial support):

```bash
pip install socio4health
```

If you don't need every feature, install `socio4health-core`. It provides the same `socio4health` package (same imports) with only the basic dependencies, and you add the features you need as extras:

```bash
pip install socio4health-core                  # basic: no scraping, ml or geo
pip install "socio4health-core[scraping]"      # + web scraping (Scrapy)
pip install "socio4health-core[ml]"            # + translation/classification (torch, transformers)
pip install "socio4health-core[geo]"           # + geospatial files (geopandas)
pip install "socio4health-core[duckdb]"        # + export to DuckDB
pip install "socio4health-core[geo,scraping]"  # any combination
pip install "socio4health-core[all]"           # same as `pip install socio4health`
```

Requires Python 3.10 or newer.

> **Upgrading from socio4health 1.0.12 or earlier:** run `pip uninstall -y socio4health` before installing the new version, so that the old package does not remove files from `socio4health-core` during the upgrade.

## Quick start

The example below downloads Brazil's 2024 PNAD Contínua, builds a categorized English dictionary of its variables, keeps only the *Business* variables for one state, and saves the result.

```python
import pandas as pd
from socio4health import Extractor, Harmonizer, s4h_export
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.utils import harmonizer_utils

# 1. Extract: crawl the IBGE page and read the fixed-width files as Dask DataFrames
extractor = Extractor(
    input_path="https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2024/",
    down_ext=[".txt", ".zip"],
    is_fwf=True,
    colnames=BraColnamesEnum.PNADC.value,
    colspecs=BraColspecsEnum.PNADC.value,
    output_path="data",
    cache_parquet=True,
    depth=0,
)
dfs = extractor.s4h_extract()

# 2. Understand: standardize, translate and classify the data dictionary
dic = harmonizer_utils.s4h_standardize_dict(pd.read_excel("raw_dictionary.xlsx"))
for col in ["question", "description", "possible_answers"]:
    dic = harmonizer_utils.s4h_translate_column(dic, col, language="en")
dic = harmonizer_utils.s4h_classify_rows(
    dic, "question_en", "description_en", "possible_answers_en",
    new_column_name="category",
    MODEL_PATH="dirreno/harmonize_bert_finetuned_classifier",
)

# 3. Harmonize: stack the quarters and select variables by theme and state
har = Harmonizer(dict_df=dic, categories=["Business"], key_col="UF", key_val=["14"])
dfs = har.s4h_vertical_merge(dfs)
selected = har.s4h_data_selector(dfs)

# 4. Load: write the harmonized table (Dask reads the data only now)
s4h_export(selected, "data/clean", format="parquet", names=["pnadc_2024"])
# or: format="csv", or format="duckdb" with path="data/surveys.duckdb"
```

To work with files you already have, pass a local folder as `input_path`. For logging, use `socio4health.s4h_set_log_level`.

More complete walkthroughs are available as notebooks:

- [Extractor](https://github.com/harmonize-tools/socio4health/blob/main/docs/source/notebooks/extractor.ipynb): online and local extraction options
- [Harmonization](https://github.com/harmonize-tools/socio4health/blob/main/docs/source/notebooks/harmonization.ipynb): the full pipeline on PNAD Contínua
- [Colombia](https://github.com/harmonize-tools/socio4health/blob/main/docs/source/notebooks/example_colombia.ipynb): DANE census with geospatial data and maps
- [Brazil](https://github.com/harmonize-tools/socio4health/blob/main/docs/source/notebooks/example_brazil.ipynb)
- [Housing deficit](https://github.com/harmonize-tools/socio4health/blob/main/docs/source/notebooks/example_housing_deficit.ipynb): a multi-country use case

## Built on

[pandas](https://pandas.pydata.org/), [Dask](https://www.dask.org/), [PyArrow](https://arrow.apache.org/docs/python/), [pyreadstat](https://pypi.org/project/pyreadstat/), [openpyxl](https://openpyxl.readthedocs.io/), [py7zr](https://py7zr.readthedocs.io/) and [requests](https://requests.readthedocs.io/). Optional extras add [Scrapy](https://scrapy.org/) (scraping), [GeoPandas](https://geopandas.org/) (geospatial), [DuckDB](https://duckdb.org/) (export) and [PyTorch](https://pytorch.org/) + [Transformers](https://huggingface.co/docs/transformers) (translation and classification).

## Resources

<details>
<summary>
Package Website
</summary>

The [socio4health website](https://harmonize-tools.github.io/socio4health/) includes the **API reference**, **user guide** and **examples**. It mainly documents the release version, but you can also find documentation for the latest development version.

</details>
<details>
<summary>
Organisation Website
</summary>

[Harmonize](https://www.harmonize-tools.org/) is an international project that develops cost-effective and reproducible digital tools for stakeholders in Latin America and the Caribbean (LAC) affected by a changing climate. These stakeholders include cities, small islands, highlands, and the Amazon rainforest.

The project consists of resources and [tools](https://harmonize-tools.github.io/) developed in conjunction with different teams from Brazil, Colombia, Dominican Republic, Peru, and Spain.

</details>

## Contributing

Bug reports and feature requests are welcome in the [issue tracker](https://github.com/harmonize-tools/socio4health/issues). Please read the [Code of Conduct](https://github.com/harmonize-tools/socio4health/blob/main/CODE_OF_CONDUCT.md) before contributing. Changes between versions are listed in the [changelog](https://github.com/harmonize-tools/socio4health/blob/main/CHANGELOG.md).

## Organizations

<table>
  <tr>
    <td align="center">
      <a href="https://www.bsc.es/" target="_blank">
        <img src="https://imgs.search.brave.com/t_FUOTCQZmDh3ddbVSX1LgHYq4mzCxvVA8U_YHywMTc/rs:fit:500:0:0/g:ce/aHR0cHM6Ly9zb21t/YS5lcy93cC1jb250/ZW50L3VwbG9hZHMv/MjAyMi8wNC9CU0Mt/Ymx1ZS1zbWFsbC5q/cGc" height="64" alt="bsc logo">
      </a>
    </td>
    <td align="center">
      <a href="https://uniandes.edu.co/" target="_blank">
        <img src="https://raw.githubusercontent.com/harmonize-tools/socio4health/refs/heads/main/docs/img/uniandes.png" height="64" alt="uniandes logo">
      </a>
    </td>
  </tr>
</table>


## Authors / Contact information

Contact the authors and contributors if you have questions or feedback.
</br>
</br>
<a href="https://github.com/dirreno">
  <img src="https://avatars.githubusercontent.com/u/39099417?v=4" style="width: 50px; height: auto;" />
</a>
<span style="display: flex; align-items: center; margin-left: 10px;">
  <strong>Diego Irreño</strong> (developer)
</span>
</br>
<a href="https://github.com/Ersebreck">
  <img src="https://avatars.githubusercontent.com/u/81669194?v=4" style="width: 50px; height: auto;" />
</a>
<span style="display: flex; align-items: center; margin-left: 10px;">
  <strong>Erick Lozano</strong> (developer)
</span>
</br>
<a href="https://github.com/Juanmontenegro99">
  <img src="https://avatars.githubusercontent.com/u/60274234?v=4" style="width: 50px; height: auto;" />
</a>
<span style="display: flex; align-items: center; margin-left: 10px;">
  <strong>Juan Montenegro</strong> (developer)
</span>
</br>
<a href="https://github.com/ingridvmoras">
  <img src="https://avatars.githubusercontent.com/u/91691844?s=400&u=945efa0d09fcc25d1e592d2a9fddb984fdc6ceea&v=4" style="width: 50px; height: auto;" />
</a>
<span style="display: flex; align-items: center; margin-left: 10px;">
  <strong>Ingrid Mora</strong> (documentation)
</span>
