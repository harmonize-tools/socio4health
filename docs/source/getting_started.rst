Getting Started
===============

Welcome to the Getting Started section of **socio4health**. This guide will walk you through the steps to install and start using the library in your projects.

Installation
------------

**socio4health** can be installed via pip from `PyPI<https://pypi.org/project/socio4health/>`_. To install the package, run the following command in your terminal:

.. code-block:: shell

   pip install socio4health

Make sure you have **Python 3.12** installed on your system; the package requires **Python version 3.10** or higher.

Usage
-----

To use the **socio4health** package, follow these steps:

1. Import the package in your Python script:

   .. code-block:: python

      from socio4health import Harmonizer
      from socio4health import Extractor

2. Create an instance of the ``Extractor`` and ``Harmonizer`` class:

   .. code-block:: python

      extractor = Extractor(input_path='path/to/input', down_ext=['.CSV'],sep=',', output_path='path/to/output')
      harmonizer = Harmonizer()



3. Use the methods provided by the classes to perform data extraction and harmonization. The main methods available in the **socio4health** package include ``extract``, ``vertical_merge``, ``drop_nan_columns``, ``get_available_columns`` and ``data_selector``. The ``extract`` method retrieves data from online or local sources, the ``vertical_merge`` consolidates a dataframe following the specifications given by the parameters, the ``get_available_columns`` retrieves the names of columns in the dataframe.

To further learn how to use the library, refer to the :doc:`examples` provided in the documentation and visit the :doc:`API_reference` for detailed descriptions of the available methods and classes.
