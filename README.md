# Harmonize Toolkit Library

[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/biomac-lab/harmonize/blob/main/README.md)
[![es](https://img.shields.io/badge/lang-es-yellow.svg)](https://github.com/biomac-lab/harmonize/blob/main/README.es.md)

The Harmonize Toolkit is a Python library that provides an easy and convenient way to access and merge data from multiple official organizations in Colombia. This library aims to simplify the process of gathering and consolidating data from different sources, enabling developers to work with up-to-date and comprehensive information.

## Features

- Access data from various Colombian official organizations
- Merge data from multiple sources into a unified dataset
- Filter and transform data based on specific criteria
- Handle data inconsistencies and discrepancies
- Export merged data in various formats (CSV, Excel, JSON, etc.)

## Installation

You can install the Harmonize Toolkit library using `pip`. Make sure you have Python 3.x installed on your system.

```shell
pip install nyctibius
```

## Usage

To use the Harmonize Toolkit library, follow these steps:

1. Import the library in your Python script:

   ```python
   from harmonize.toolkit import Toolkit
   ```

2. Create an instance of the `Toolkit` class:

   ```python
   toolkit = Toolkit()
   ```

3. Access and merge data from different official organizations:

   ```python
   data_source_1 = toolkit.load_data('data_source_1')
   data_source_2 = toolkit.load_data('data_source_2')
   merged_data = toolkit.merge_data([data_source_1, data_source_2])
   ```

4. Filter and transform data as needed:

   ```python
   filtered_data = toolkit.filter_data(merged_data, 'column_name', 'value')
   transformed_data = toolkit.transform_data(filtered_data, 'column_name', transformation_function)
   ```

5. Export the merged and processed data:

   ```python
   toolkit.save_data(merged_data, 'output.csv')
   ```

## Supported Data Sources

The Harmonize Toolkit library supports the following official organizations:

- National Administrative Department of Statistics (DANE)
- Ministry of Health and Social Protection (Minsalud)
- Instituto Nacional de Salud de Colombia (INS)
- Datos Abiertos

Please note that accessing data from these organizations may require authentication or specific credentials. Make sure you have the necessary permissions before using the library.


## License

The Harmonize Toolkit library is open-source and released under the [MIT License](https://opensource.org/licenses/MIT). Feel free to use, modify, and distribute this library in accordance with the terms of the license.

## Acknowledgements

We would like to thank the following official organizations for providing the data used by the Harmonize Toolkit library:

- National Administrative Department of Statistics (DANE)
- Ministry of Health and Social Protection (Minsalud)
- Instituto Nacional de Salud de Colombia (INS)
- Datos Abiertos

Without their efforts in collecting and publishing data, this library would not be possible.

## Contact

For any questions, suggestions, or feedback regarding the Harmonize Toolkit library, please contact:

Cristian Amaya
Email: cm.amaya10@uniandes.edu.co

## Disclaimer

This library is not officially affiliated with or endorsed by any of the mentioned official organizations. The data provided by this library is sourced from publicly available information and may not always reflect the most current or accurate data. Please verify the information with the respective official sources for critical use cases.