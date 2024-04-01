---
title: Features
layout: page
nav_order: 4
---

# Getting Started

Welcome to the Getting Started section of Nyctibius. This guide will walk you through the steps to install and start using the library in your projects.

## Installation

To install Nyctibius, you can use the following command:

```shell
pip install nyctibius
```

Make sure you have Python 3.x installed on your system; the package requires Python version 3.7 or higher.

## Usage

To use the Nyctibius package, follow these steps:

1. Import the package in your Python script:

   ```python
   from nyctibius import Harmonizer
   ```

2. Create an instance of the `Harmonizer` class:

   ```python
    harmonizer = Harmonizer()
    ```

Harmonize has three core functionalities: `extract`, `load`, and `transform`. The `extract` method retrieves data from online or sources,  the `transform` method modifies the data in different formats into the same dataframe structure, and the `load` method merges the data into a relational database.

3. Extract data from online sources and create a list of data information:

   ```python
   url = 'https://www.example.com'
   depth = 0
   ext = 'csv'
   list_datainfo = harmonizer.extract(url=url, depth=depth, ext=ext)
   harmonizer = Harmonizer(list_datainfo)
   ```

4. Transform the data extracted from different sources into the same dataframe structure:

   ```python
   harmonizer.transform()
   ```
   
5. Load the data from the list of data information and merge it into a relational database:

   ```python
    results = harmonizer.load()
    ```

After loading the data, you can perform modifications and queries on the database. The `Modifier` class allows you to modify the database, while the `Querier` class enables you to query the database.

6. Import the modifier module and create an instance of the `Modifier` class:

    ```python
    from nyctibius.db.modifier import Modifier
    modifier = Modifier(db_path='_path_/data/output/nyctibius.db')
    ```

7. Perform modifications:

    ```python
    tables = modifier.get_tables()
    print(tables)
    ```

8. Import the querier module and create an instance of the `Querier` class:

    ```python
    from nyctibius.db.querier import Querier
    querier = Querier(db_path='_path_/data/output/nyctibius.db')
    ```

9. Perform queries:

    ```python
    querier.query('SELECT * FROM table_name')
    ```