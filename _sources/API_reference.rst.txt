API Reference
=============

This section contains the Documentation of the Application Programming
Interface (**API**) of **socio4health**. The information in this section is automatically
created from the documentation strings in the original **Python** code.

.. currentmodule:: socio4health

Extractor
~~~~~~~~~

**Methods**

.. autosummary::
   :toctree:
   :nosignatures:

   Extractor
   extractor.s4h_get_default_data_dir
   Extractor.s4h_extract
   Extractor.s4h_delete_download_folder

Harmonizer
~~~~~~~~~~

**Methods**

.. autosummary::
   :toctree:
   :nosignatures:

   Harmonizer
   Harmonizer.s4h_vertical_merge
   Harmonizer.s4h_drop_nan_columns
   Harmonizer.s4h_get_available_columns
   Harmonizer.s4h_harmonize_dataframes
   Harmonizer.s4h_data_selector
   Harmonizer.s4h_compare_with_dict
   Harmonizer.s4h_join_data

Utils
~~~~~~~~~~
**Extractor**

.. autosummary::
   :toctree:
   :nosignatures:

   utils.extractor_utils.compressed2files
   utils.extractor_utils.download_request
   utils.extractor_utils.s4h_parse_fwf_dict
   utils.extractor_utils.run_standard_spider

**Harmonizer**

.. autosummary::
   :toctree:
   :nosignatures:

   utils.harmonizer_utils.s4h_classify_rows
   utils.harmonizer_utils.s4h_get_classifier
   utils.harmonizer_utils.s4h_standardize_dict
   utils.harmonizer_utils.s4h_translate_column

**Spider**

.. autosummary::
   :toctree:
   :nosignatures:

    utils.standard_spider.StandardSpider
    utils.standard_spider.StandardSpider.parse
    utils.standard_spider.StandardSpider.parse_item

Enums 
~~~~~~~~~~
.. autosummary::
   :toctree:
   :nosignatures:

   enums.data_info_enum
   enums.data_info_enum.BraColspecsEnum
   enums.data_info_enum.CountryEnum
   enums.data_info_enum.NameEnum
   enums.dict_enum.ColumnMappingEnum
