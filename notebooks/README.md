# Example notebooks

Run them from this folder (`jupyter lab` here, or open them in Binder/Colab from their badges). They are also published in the [documentation](https://harmonize-tools.github.io/socio4health/examples.html), which copies them on every build.

```
notebooks/
├── *.ipynb
├── inputs/              raw data dictionaries the notebooks read (versioned)
└── data/                created when the notebooks run (ignored by git)
    └── <notebook>/
        ├── raw/         files downloaded by Extractor
        └── output/      tables the notebook produces
```

| Notebook | Reads | Writes |
|---|---|---|
| `extractor` | — | `data/extractor/raw/` |
| `harmonization` | `inputs/raw_dictionary.xlsx` | `data/harmonization/raw/`, `data/harmonization/output/PNADC_2024_harmonized.csv` |
| `example_colombia` | `inputs/raw_dic_mzn.xlsx` | `data/example_colombia/raw/` |
| `example_brazil` | `inputs/raw_dictionary_br_2010.xlsx` | `data/example_brazil/raw/` |
| `example_housing_deficit` | — | `data/example_housing_deficit/raw/`, `data/example_housing_deficit/output/run_<id>/` |
