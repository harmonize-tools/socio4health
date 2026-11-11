# socio4health

Socio4health is a Python package for gathering and consolidating socio-demographic data.

This is the **full** distribution: it installs `socio4health-core` together with every
optional feature (web scraping, machine learning/NLP and geospatial support).

```
pip install socio4health
```

## Lighter installations

If you don't need every feature, install `socio4health-core` instead. It provides the same
`socio4health` package with only the basic dependencies, and you can add extras as needed:

```
pip install socio4health-core                 # basic: no scraping, ml or geo
pip install "socio4health-core[scraping]"     # + web scraping (Scrapy)
pip install "socio4health-core[ml]"           # + translation/classification (torch, transformers)
pip install "socio4health-core[geo]"          # + geospatial files (geopandas)
pip install "socio4health-core[geo,scraping]" # any combination
```

Documentation and source: https://github.com/harmonize-tools/socio4health
