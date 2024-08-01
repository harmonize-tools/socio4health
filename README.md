# socio4health <a href='https://www.harmonize-tools.org/'><img src='https://harmonize-tools.github.io/harmonize-logo.png' align="right" height="139" /></a>

<!-- badges: start -->

[![Lifecycle:
maturing](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![MIT
license](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/harmonize-tools/socio4health/blob/main/LICENSE.md/)
[![GitHub
contributors](https://img.shields.io/github/contributors/harmonize-tools/socio4health)](https://github.com/harmonize-tools/socio4health/graphs/contributors)
[![GitHub
commits](https://badgen.net/github/commits/harmonize-tools/socio4health/main)
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
      <a href="https://cran.r-project.org/web/packages/dplyr/index.html" target="_blank">
        <img src="https://tidyverse.tidyverse.org/logo.png" height="50" alt="dplyr logo">
      </a>
    </td>
    <td align="left">
      <strong>dplyr</strong><br>
      Provides a set of tools for efficiently manipulating datasets in R.<br>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://cran.r-project.org/web/packages/ggplot2/index.html" target="_blank">
        <img src="https://imgs.search.brave.com/7xErK1yv_WwEZ-syGmCUbH4n1THQcF7ukwTLS42zAyM/rs:fit:860:0:0/g:ce/aHR0cHM6Ly9yLWdy/YXBoLWdhbGxlcnku/Y29tL2ltZy9vdGhl/ci9nZ3Bsb3QySGV4/LmpwZw" height="50" alt="ggplot2 logo">
      </a>
    </td>
    <td align="left">
      <strong>ggplot2</strong><br>
      Used for creating complex plots from data in a data frame.<br>
    </td>
  </tr>
</table>



## Installation

You can install the latest version of the package from GitHub using the `remotes` package:

```R
# Install remotes if you haven't already
install.packages("remotes")

# Install the package from GitHub
remotes::install_github("your_username/your_package_name")
```

## How to Use it / Vignette

This document provides a guide to working with NetCDF files in R using the `ncdf4` and `raster` packages. The following example demonstrates how to read a NetCDF file, extract data, and visualize it.

<details>
<summary>
  Vignette
</summary>
  
## Prerequisites

Before running the script, ensure you have the necessary packages installed. You can install them using the following commands:

```r
install.packages("ncdf4")
install.packages("raster")
install.packages("ggplot2")
```

## R script
```r
# Load necessary libraries
library(ncdf4)
library(raster)
library(ggplot2)

# Set the path to your NetCDF file
nc_file <- "path/to/your/file.nc"

# Open the NetCDF file
nc <- nc_open(nc_file)

# Print the NetCDF file summary
print(nc)

# Extract data from a specific variable (e.g., 'temperature')
# Replace 'temperature' with the actual variable name in your NetCDF file
var_name <- "temperature"
temperature <- ncvar_get(nc, var_name)

# Get the dimensions of the data
lon <- ncvar_get(nc, "lon")
lat <- ncvar_get(nc, "lat")
time <- ncvar_get(nc, "time")

# Close the NetCDF file
nc_close(nc)

# Create a raster layer for the first time step (if applicable)
# Modify the indexing based on your data structure
r <- raster(t(temperature[,,1]), xmn=min(lon), xmx=max(lon), ymn=min(lat), ymx=max(lat), crs=CRS("+proj=longlat +datum=WGS84"))

# Plot the raster layer using base R plot
plot(r, main=paste("Temperature at Time Step 1"))

# Convert the raster to a data frame for ggplot2 visualization
r_df <- as.data.frame(r, xy=TRUE)

# Plot the raster layer using ggplot2
ggplot(r_df, aes(x=x, y=y, fill=layer)) +
  geom_raster() +
  coord_fixed() +
  scale_fill_viridis_c() +
  labs(title="Temperature at Time Step 1", x="Longitude", y="Latitude", fill="Temperature") +
  theme_minimal()
```
</details>

## Resources

<details>
<summary>
Package Website
</summary>

The [`example` website](https://cran.r-project.org/) package website includes a function reference, a model outline, and case studies using the package. The site mainly concerns the release version, but you can also find documentation for the latest development version.

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
<a href="https://github.com/drrachellowe">
  <img src="https://imgs.search.brave.com/5LHcD0fArBHiqOOzb1AlCj7YGRHVMHCZcK_kYao0aos/rs:fit:500:0:0:0/g:ce/aHR0cHM6Ly9jZG4t/aWNvbnMtcG5nLmZy/ZWVwaWsuY29tLzI1/Ni80NjYxLzQ2NjEz/MTgucG5nP3NlbXQ9/YWlzX2h5YnJpZA" style="width: 50px; height: auto;" />
</a>
<span style="display: flex; align-items: center; margin-left: 10px;">
  <strong>Rachel Lowe</strong> (developer)
  <a href="https://orcid.org/0000-0003-3939-7343" style="margin-left: 10px;">
    <img src="https://orcid.org/sites/default/files/images/orcid_16x16.png" alt="ORCID" style="width: 16px; height: 16px;" />
  </a>
</span>

## Citation

- **APA Format:**
  - Lowe, R. (2020). *How to use the NetCDF files*. Package documentation. Retrieved from https://cran.r-project.org/).
