<a href='https://harmonize-tools.github.io/socio4health/'><img height="100" src="https://raw.githubusercontent.com/harmonize-tools/socio4health/main/docs/source/_static/image.png"/></a>

# socio4health <a href='https://www.harmonize-tools.org/'><img height="139" src="https://harmonize-tools.github.io/harmonize-logo.png"/></a>

<!-- badges: start -->

[![Lifecycle:
maturing](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![MIT
license](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/harmonize-tools/socio4health/blob/main/LICENSE.md/)
[![GitHub
contributors](https://img.shields.io/github/contributors/harmonize-tools/socio4health)](https://github.com/harmonize-tools/socio4health/graphs/contributors)
![commits](https://badgen.net/github/commits/harmonize-tools/socio4health/main)
<!-- badges: end -->

## Overview
<p style="font-family: Arial, sans-serif; font-size: 14px;">
  Package socio4health is an extraction, transformation, loading (ETL), and AI-assisted query and visualization (AI QV) tool designed to simplify the intricate process of collecting and merging data 📊 from multiple sources, focusing on sociodemographic and census datasets from Colombia, Brazil, and Peru, into a unified relational database structure.
</p>

- Seamlessly retrieve data from online data sources through web scraping, as well as from local files.
- Support for various data formats, including `.csv`, `.xlsx`, `.xls`, `.txt`, `.sav`, and compressed files, ensuring versatility in sourcing information.
- Consolidating extracted data into a pandas DataFrame.
- Consolidating transformed data into a cohesive relational database.
- Conduct precise queries and apply transformations to meet specific criteria.



## Dependencies

<table>
  <tr>
    <td align="center">
      <a href="https://pandas.pydata.org/" target="_blank">
        <img src="https://avatars.githubusercontent.com/u/21206976?s=280&v=4" height="50" alt="pandas logo">
      </a>
    </td>
    <td align="left">
      <strong>Pandas</strong><br>
      Pandas is a fast, powerful, flexible, and easy-to-use open source data analysis and manipulation tool.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://numpy.org/" target="_blank">
        <img src="https://avatars.githubusercontent.com/u/288276?s=48&v=4" height="50" alt="numpy logo">
      </a>
    </td>
    <td align="left">
      <strong>Numpy</strong><br>
      The fundamental package for scientific computing with Python.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://scrapy.org/" target="_blank">
        <img src="https://avatars.githubusercontent.com/u/733635?s=48&v=4" height="50" alt="scrapy logo">
      </a>
    </td>
    <td align="left">
      <strong>Scrapy</strong><br>
      Framework for extracting the data you need from websites.<br>
    </td>
  </tr>
</table>

- <a href="https://openpyxl.readthedocs.io/en/stable/">openpyxl</a>
- <a href="https://py7zr.readthedocs.io/en/latest/">py7zr</a>
- <a href="https://pypi.org/project/pyreadstat/">pyreadstat</a>
- <a href="https://tqdm.github.io/">tqdm</a>
- <a href="https://requests.readthedocs.io/en/latest/">requests</a>

## Installation

**socio4health** can be installed via pip from [PyPI](https://pypi.org/project/socio4health/).

```python
# Install using pip
pip install socio4health
```

## How to Use it

To use the socio4health package, follow these steps:

1. Import the package in your Python script:

   ```python
   from socio4health import Extractor()
   from socio4health import Harmonizer
   
   ```
2. Create an instance of the `Extractor` class:

   ```python
   extractor = Extractor()
   ```

3. Extract data from online sources and create a list of data information:

   ```python
   url = 'https://www.example.com'
   depth = 0
   ext = 'csv'
   list_datainfo = extractor.s4h_extract(url=url, depth=depth, ext=ext)
   harmonizer = Harmonizer()
   ```

## Resources

<details>
<summary>
Package Website
</summary>

The [socio4health website](https://harmonize-tools.github.io/socio4health/) package website includes **API reference**, **user guide**, and **examples**. The site mainly concerns the release version, but you can also find documentation for the latest development version.

</details>
<details>
<summary>
Organisation Website
</summary>

[Harmonize](https://www.harmonize-tools.org/) is an international project that develops cost-effective and reproducible digital tools for stakeholders in Latin America and the Caribbean (LAC) affected by a changing climate. These stakeholders include cities, small islands, highlands, and the Amazon rainforest.

The project consists of resources and [tools](https://harmonize-tools.github.io/) developed in conjunction with different teams from Brazil, Colombia, Dominican Republic, Peru, and Spain.

</details>

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

Here is the contact information of authors/contributors in case users have questions or feedback.
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
