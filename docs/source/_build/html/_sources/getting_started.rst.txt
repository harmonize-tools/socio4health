Getting Started
===============

Welcome to the Getting Started section of socio4health. This guide will walk you through the steps to install and start using the library in your projects.

Installation
------------

To install socio4health, use the following command:

.. code-block:: shell

   pip install socio4health

Make sure you have Python 3.12 installed on your system; the package requires Python version 3.7 or higher.

Usage
-----

To use the socio4health package, follow these steps:

1. Import the package in your Python script:

   .. code-block:: python

      from socio4health import Harmonizer

2. Create an instance of the ``Harmonizer`` class:

   .. code-block:: python

      harmonizer = Harmonizer()

Harmonizer has three core functionalities: ``extract``, ``load``, and ``transform``. The ``extract`` method retrieves data from online or local sources, the ``transform`` method modifies the data from different formats into the same dataframe structure, and the ``load`` method merges the data into a relational database.

3. Extract data from online sources and create a list of data information:

   .. code-block:: python

      url = 'https://www.example.com'
      depth = 0
      ext = 'csv'
      list_datainfo = harmonizer.extract(url=url, depth=depth, ext=ext)
      harmonizer = Harmonizer(list_datainfo)

4. Transform the data extracted from different sources into the same dataframe structure:

   .. code-block:: python

      harmonizer.transform()

5. Load the data from the list of data information and merge it into a relational database:

   .. code-block:: python

      results = harmonizer.load()

After loading the data, you can perform modifications and queries on the database. The ``Modifier`` class allows you to modify the database, while the ``Querier`` class enables you to query the database.

6. Import the modifier module and create an instance of the ``Modifier`` class:

   .. code-block:: python

      from socio4health.db.modifier import Modifier
      modifier = Modifier(db_path='_path_/data/output/nyctibius.db')

7. Perform modifications:

   .. code-block:: python

      tables = modifier.get_tables()
      print(tables)

8. Import the querier module and create an instance of the ``Querier`` class:

   .. code-block:: python

      from socio4health.db.querier import Querier
      querier = Querier(db_path='_path_/data/output/nyctibius.db')

9. Perform queries:

   .. code-block:: python

      querier.query('SELECT * FROM table_name')