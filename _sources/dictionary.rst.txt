How to create a raw dictionary for data harmonization
=====================================================

For data harmonization, it is **essential** to have a **raw dictionary** that defines the **structure** and **content** of the data. This dictionary serves as a reference for understanding the data fields, their types, and any transformations that may be necessary. In this section, we will outline the steps to create a **raw dictionary** for **data harmonization**.

1. Retrieve dictionary from data sources
-------------------------------------

Data sources usually provide a dictionary that describes the data fields. These dictionaries can be in various formats, such as ``JSON``, ``XML``, or ``CSV``. The first step is to retrieve the dictionary from the data sources you are working with.

In the following example, we create a raw dictionary from a ``CSV`` file containing the data field definitions from Colombia's 2018 National Population and Housing Census (**CNPV**), published by the National Administrative Department of Statistics (**DANE**).
The CNPV-2018 CSV file is available `here <https://microdatos.dane.gov.co/index.php/catalog/643/download/12620>`_.

.. image:: ../img/csv.png
   :align: center
   :width: 600px
   :alt: CSV file structure

In this case, clicking the link downloads a ``ZIP`` file containing the ``CSV`` file. Once the ``CSV`` file is extracted, we can inspect it to understand its structure and the fields it contains.
The CSV file contains two sheets, one with the variables and another with the categories. The first sheet contains the variable definitions, while the second sheet contains the possible answers for each variable. We must ensure that we include both sheets in our raw dictionary.

.. note::
   Each data source and survey may have its own format for the dictionary, so **it is important to understand the structure of the dictionary you are working with**. We might need to include all the data contained in the file that fills the required columns in the raw dictionary.
   These columns are: **question**, **variable_name**, **value**, **description**, and **possible_answers**.

.. important::
   Be aware that the columns may have **different names** and may appear in **different order** in the original dictionary, so you may need to adapt them accordingly.

2. Create the raw dictionary
-------------------------

Once you have retrieved the dictionary from the data sources, the next step is, in a empty Excel (``xlx``) sheet, fill the first row with the following columns: ``question``, ``variable_name``, ``value``, ``description``, and ``possible_answers`` in the first row in the same order they appear in the provided data dictionary. **Include any additional columns that may be relevant for your specific use case**.
In this case, the columns are in the following order:

.. image:: ../img/raw_dictionary.png
   :align: center
   :width: 600px
   :alt: Raw dictionary structure


Then, copy the data from the original dictionary to the new raw dictionary, ensuring that the data is placed in the **correct columns**.

In this example, we copy the data from the **two sheets** in the CNPV-2018 CSV file to the new raw dictionary, ensuring that the data is placed in the **correct** columns and keeping the same order.

.. image:: ../img/dict.png
   :align: center
   :width: 600px
   :alt: Raw dictionary

With the raw dictionary created, we can now proceed to the next step of data harmonization, which involves mapping the **raw dictionary** to a **standardized format**. This is covered in the `Harmonization <https://harmonize-tools.github.io/socio4health/notebooks/harmonization.html>`_ tutorial.

.. toctree::
   :maxdepth: 1