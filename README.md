# socio4health <a href='https://www.harmonize-tools.org/'><img src='https://harmonize-tools.github.io/harmonize-logo.png' align="right" height="139" /></a>

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
  Welcome to the Harmonize Health Project repository. This repository is dedicated to improving health outcomes by integrating various health data sources into a unified framework. Our tools and documentation are designed to facilitate data harmonization, analysis, and reporting for health researchers and practitioners. 
</p>
<p style="font-family: Arial, sans-serif; font-size: 14px;">
  <em>Join us in our mission to harmonize health data and enhance the quality of healthcare research.</em>
</p>


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
      Pandas is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool.<br>
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
  <tr>
    <td align="center">
      <a href="https://pandas-ai.com/" target="_blank">
        <img src="https://avatars.githubusercontent.com/u/154438448?s=48&v=4" height="50" alt="ggplot2 logo">
      </a>
    </td>
    <td align="left">
      <strong>Pandasai</strong><br>
      Integrates generative artificial intelligence capabilities into pandas, making dataframes conversational.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://openpyxl.readthedocs.io/en/stable/" target="_blank">
        <img src="https://github.com/harmonize-tools/socio4health/blob/main/docs/img/external-link-alt.svg" height="50" alt="external link">
      </a>
    </td>
    <td align="left">
      <strong>openpyxl</strong><br>
      Read/write Excel 2010 xlsx/xlsm/xltx/xltm files.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://py7zr.readthedocs.io/en/latest/" target="_blank">
        <img src="https://github.com/harmonize-tools/socio4health/blob/main/docs/img/external-link-alt.svg" height="50" alt="external link">
      </a>
    </td>
    <td align="left">
      <strong>py7zr</strong><br>
      Used for uncompress zip files.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://pypi.org/project/pyreadstat/" target="_blank">
        <img src="https://github.com/harmonize-tools/socio4health/blob/main/docs/img/external-link-alt.svg" height="50" alt="external link">
      </a>
    </td>
    <td align="left">
      <strong>pyreadstat</strong><br>
      ead and write SAS (sas7bdat, sas7bcat, xport/xpt), SPSS (sav, zsav, por) and Stata (dta) files into/from pandas data frames.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://tqdm.github.io/" target="_blank">
        <img src="https://github.com/harmonize-tools/socio4health/blob/main/docs/img/external-link-alt.svg" height="50" alt="external link">
      </a>
    </td>
    <td align="left">
      <strong>tqdm</strong><br>
      Used for console loading bars.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://requests.readthedocs.io/en/latest/" target="_blank">
        <img src="https://github.com/harmonize-tools/socio4health/blob/main/docs/img/external-link-alt.svg" height="50" alt="external link">
      </a>
    </td>
    <td align="left">
      <strong>requests</strong><br>
      HTTP library for Python, built for human beings.<br>
    </td>
  </tr>
</table>



## Installation

You can install the latest version of the package from GitHub using the `remotes` package:

```R
# Install using pip
pip install nyctibius
```

## How to Use it

To use the Nyctibius package, follow these steps:

1. Import the package in your Python script:

   ```python
   from nyctibius import Harmonizer
   ```

2. Create an instance of the `Harmonizer` class:

   ```python
   harmonizer = Harmonizer()
   ```

3. Extract data from online sources and create a list of data information:

   ```python
   url = 'https://www.example.com'
   depth = 0
   ext = 'csv'
   list_datainfo = harmonizer.extract(url=url, depth=depth, ext=ext)
   harmonizer = Harmonizer(list_datainfo)
   ```

4. Load the data from the list of data information and merge it into a relational database:

   ```python
   results = harmonizer.load()
   ```

5. Import the modifier module and create an instance of the `Modifier` class:

   ```python
   from nyctibius.db.modifier import Modifier
   modifier = Modifier(db_path='../../data/output/nyctibius.db')
   ```
   
6. Perfom modifications:

   ```python
   tables = modifier.get_tables()
   print(tables)
   ```
   
7. Import the querier module and create an instance of the `Querier` class:

   ```python
   from nyctibius.db.querier import Querier
   querier = Querier(db_path='data/output/nyctibius.db')
   ```

8. Perform queries:

   ```python
   df = querier.select(table="Estructura CHC_2017").execute()
   print(df)
   ```

## Resources

<details>
<summary>
Package Website
</summary>

The [socio4health website](https://ersebreck.github.io/Nyctibius/) package website includes a function reference, a model outline, and case studies using the package. The site mainly concerns the release version, but you can also find documentation for the latest development version.

</details>
<details>
<summary>
Organisation Website
</summary>

[Harmonize](https://www.harmonize-tools.org/) is an international develop cost-effective and reproducible digital tools for stakeholders in hotspots affected by a changing climate in Latin America & the Caribbean (LAC), including cities, small islands, highlands, and the Amazon rainforest.

The project consists of resources and [tools](https://harmonize-tools.github.io/) developed in conjunction with different teams from Brazil, Colombia, Dominican Republic, Peru and Spain.

</details>

## Organizations

<table>
  <tr>
    <td align="center">
      <a href="https://www.bsc.es/" target="_blank">
        <img src="https://imgs.search.brave.com/t_FUOTCQZmDh3ddbVSX1LgHYq4mzCxvVA8U_YHywMTc/rs:fit:500:0:0/g:ce/aHR0cHM6Ly9zb21t/YS5lcy93cC1jb250/ZW50L3VwbG9hZHMv/MjAyMi8wNC9CU0Mt/Ymx1ZS1zbWFsbC5q/cGc" height="64" alt="dplyr logo">
      </a>
    </td>
    <td align="left">
      <strong>GHR</strong><br>
      Global Health Resilience
    </td>
  </tr>
</table>


## Authors / Contact information

List the authors/contributors of the package and provide contact information if users have questions or feedback.
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
