Getting Started
===============

Welcome to the Getting Started section of **socio4health**. This guide will walk you through the steps to install and start using the library in your projects.

Installation
------------

Install the full package from `PyPI <https://pypi.org/project/socio4health/>`_ (includes web scraping, machine learning and geospatial support):

.. code-block:: shell

   pip install socio4health

If you don't need every feature, install ``socio4health-core``. It provides the same ``socio4health`` package (same imports) with only the basic dependencies, and you add the features you need as extras:

.. code-block:: shell

   pip install socio4health-core                  # basic: no scraping, ml or geo
   pip install "socio4health-core[scraping]"      # + web scraping (Scrapy)
   pip install "socio4health-core[ml]"            # + translation/classification (torch, transformers)
   pip install "socio4health-core[geo]"           # + geospatial files (geopandas)
   pip install "socio4health-core[duckdb]"        # + export to DuckDB
   pip install "socio4health-core[geo,scraping]"  # any combination
   pip install "socio4health-core[all]"           # same as `pip install socio4health`

**socio4health** requires **Python 3.10** or newer.

Usage
-----

To use the **socio4health** package, follow these steps:

1. Import the package in your Python script:

   .. code-block:: python

      from socio4health import Extractor, Harmonizer, s4h_export

2. Create an instance of the ``Extractor`` and ``Harmonizer`` class:

   .. code-block:: python

      extractor = Extractor(input_path='path/to/input', down_ext=['.csv'], sep=',', output_path='path/to/output')
      harmonizer = Harmonizer()

3. Use the methods provided by the classes to perform data extraction and harmonization. The main methods available in the **socio4health** package include ``s4h_extract``, ``s4h_vertical_merge``, ``s4h_drop_nan_columns``, ``s4h_get_available_columns`` and ``s4h_data_selector``. The ``s4h_extract`` method retrieves data from online or local sources, the ``s4h_vertical_merge`` consolidates a dataframe following the specifications given by the parameters, the ``s4h_get_available_columns`` retrieves the names of columns in the dataframe.

4. Save the result with ``s4h_export`` as Parquet, CSV or tables in a DuckDB database:

   .. code-block:: python

      dfs = extractor.s4h_extract()
      s4h_export(harmonizer.s4h_vertical_merge(dfs), 'path/to/clean', format='parquet')

Harmonized records and shares
-----------------------------

For household surveys of several countries and years, **socio4health** can build one table of households and one of persons with harmonized variables and an expansion factor, and summarize them per subnational unit. Two JSON files describe the work:

- A **schema** with the harmonized variables: the table each one belongs to, its type and the label of each code.

  .. code-block:: json

     {
       "AREA":  {"DESCRIPTION": "Housing area type", "LEVEL": ["households", "persons"],
                 "VALUES": {"1": "Urban", "2": "Rural", "0": "Missing"}},
       "P_AGE": {"DESCRIPTION": "Age in years", "TYPE": "numeric", "UNIT": "years"},
       "P_EDUCATION": {"DESCRIPTION": "Educational attainment", "UNIVERSE": {"P_AGE": [25, null]},
                       "VALUES": {"1": "Primary", "2": "Secondary", "0": "Missing"}}
     }

  Variables starting with ``H_`` belong to the households table and those starting with ``P_`` to the persons table; ``UNIVERSE`` limits the records a variable is summarized on (here, people aged 25 or over).

- The **surveys**: where the files of each year are, how to read them, their column and value mappings, household and person keys, weights and survey rules (see :class:`socio4health.survey.Survey`).

  .. code-block:: json

     {
       "GEIH": {
         "country": "COL", "years": [2023, 2024],
         "parts": [{"path": "GEIH/{year}"}],
         "reader": {"down_ext": [".csv"], "sep": ";"},
         "column_mapping": "col_mapping/column_mapping.json",
         "value_mapping": "col_mapping/geih_mapping.json",
         "factor": {"factor_col": "FEX_C18", "id_col": "DIRECTORIO"},
         "household_key": [["DIRECTORIO", "SECUENCIA_P"]],
         "person_key": [["ORDEN"]]
       }
     }

Then build the records of each year and the weighted shares per unit:

.. code-block:: python

   import pandas as pd
   from socio4health import Schema, s4h_build_records, s4h_export, s4h_load_surveys, s4h_shares

   if __name__ == "__main__":   # needed for workers > 1 on Windows and macOS
       schema = Schema.from_json("harmonized_mapping.json")
       surveys = s4h_load_surveys("surveys.json", data_root="data/raw")

       # data/records/households/<year>/ and data/records/persons/<year>/; years already done are skipped
       report = s4h_build_records(surveys["GEIH"], schema, "data/records", workers=2)

       tables = {level: pd.read_parquet(f"data/records/{level}/2024") for level in ("households", "persons")}
       shares = s4h_shares(tables, schema, by=["YEAR", "ADMIN_DIVISION"])
       s4h_export({"shares": shares, "codebook": schema.codebook()}, "data/clean", format="csv")

``s4h_shares`` gives, for each unit, the weighted share of each category of every variable (the categories add up to 1), the mean of numeric variables and the population and number of households. ``s4h_check_records`` reports duplicate IDs, records without weight and codes that are not in the schema.

To further learn how to use the library, refer to the :doc:`examples` provided in the documentation and visit the :doc:`API_reference` for detailed descriptions of the available methods and classes.
