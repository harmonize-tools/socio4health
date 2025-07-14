Getting Started
===============

Welcome to the Getting Started section of socio4health. This guide will walk you through the steps to install and start using the library in your projects.

Installation
------------

To install socio4health, use the following command:

.. code-block:: shell

   pip install socio4health

Make sure you have Python 3.12 installed on your system; the package requires Python version 3.10 or higher.

Usage
-----

To use the socio4health package, follow these steps:

1. Import the package in your Python script:

   .. code-block:: python

      from socio4health import Harmonizer
      from socio4health import Extractor

2. Create an instance of the ``Extractor`` and ``Harmonizer`` class:

   .. code-block:: python

      extractor = Extractor(input_path='path/to/input', down_ext=['.CSV'],sep=',', output_path='path/to/output')
      harmonizer = Harmonizer()

The library has the following functionalities: ``extract``, ``vertical_merge``, ``drop_nan_columns``, ``get_available_columns`` and ``data_selector``. The ``extract`` method retrieves data from online or local sources, the ``vertical_merge`` consolidates a dataframe with the specifications given by the parameters, the ``get_available_columns`` retrieves the names of columns in the consolidated dataframe'.

3. Extract data from online sources and create a list of data information:

   .. code-block:: python

      url = 'https://www.example.com'
      depth = 0
      ext = 'csv'
      list_datainfo = harmonizer.extract(url=url, depth=depth, ext=ext)
      harmonizer = Harmonizer(list_datainfo)

