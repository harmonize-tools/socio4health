API Reference
=============

This section contains the Documentation of the Application Programming
Interface (API) of socio4health. The information in this section is automatically
created from the documentation strings in the original Python code. In the
left-hand menu you will find the different categories of the API documentation.

.. currentmodule:: socio4health

Extractor
~~~~~~~~~

**Methods**

.. autosummary::
   :toctree:

   Extractor
   extractor.get_default_data_dir
   Extractor.extract
   Extractor.delete_download_folder

Harmonizer
~~~~~~~~~~

**Methods**

.. autosummary::
   :toctree: 

   Harmonizer.vertical_merge
   Harmonizer.drop_nan_columns
   Harmonizer.get_available_columns
   Harmonizer.harmonize_dataframes
   Harmonizer.data_selector

Utils
~~~~~~~~~~
**Extractor**

.. autosummary::
   :toctree: 

   utils.extractor_utils.compressed2files
   utils.extractor_utils.download_request
   utils.extractor_utils.parse_pnadc_sas_script
   utils.extractor_utils.run_standard_spider

**Harmonizer**

.. autosummary::
   :toctree:


   utils.harmonizer_utils.classify_rows
   utils.harmonizer_utils.get_classifier
   utils.harmonizer_utils.standardize_dict
   utils.harmonizer_utils.translate_column

Enums 
~~~~~~~~~~
.. autosummary::
   :toctree:

   enums.data_info_enum
   enums.data_info_enum.BraColspecsEnum
   enums.data_info_enum.CountryEnum
   enums.data_info_enum.NameEnum
   enums.dict_enum.ColumnMappingEnum
   enums.data_info_enum.BraColspecsEnum
   enums.data_info_enum.CountryEnum
   enums.data_info_enum.NameEnum
   enums.dict_enum.ColumnMappingEnum
