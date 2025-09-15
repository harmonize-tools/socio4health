socio4health
============

.. |lifecycle| image:: https://img.shields.io/badge/lifecycle-experimental-orange.svg
   :target: https://lifecycle.r-lib.org/articles/stages.html#experimental
   :alt: Lifecycle: experimental

.. |license| image:: https://img.shields.io/badge/License-MIT-blue.svg
   :target: https://github.com/harmonize-tools/socio4health/blob/main/LICENSE.md
   :alt: MIT license

.. |contributors| image:: https://img.shields.io/github/contributors/harmonize-tools/socio4health
   :target: https://github.com/harmonize-tools/socio4health/graphs/contributors
   :alt: GitHub contributors

.. |commits| image:: https://badgen.net/github/commits/harmonize-tools/socio4health/main
   :alt: commits

|lifecycle| |license| |contributors| |commits|

Welcome to the official documentation site for **socio4health**\! This site serves as the central hub for all documentation, resources, and updates related to the library.

.. grid::

    .. grid-item-card:: User Guide
        :link: user_guide
        :link-type: doc

        User guide on installation and the basic concepts of **socio4health**.

    .. grid-item-card:: Examples
        :link: examples
        :link-type: doc

        Examples of **socio4health** usage.

    .. grid-item-card:: API Reference
        :link: API_reference
        :link-type: doc

        **socio4health** application programming interface (API) reference.

    .. grid-item-card:: More Harmonize
        :link: https://github.com/harmonize-tools

        Find out more useful resources developed by **Harmonize** project on GitHub\!

----

Introduction
------------

The Python package **socio4health** is an **extraction**, **transformation**, **loading**, and **AI-assisted query** and **visualization** (ETL-AI QV) tool designed to simplify the process of collecting and merging data from multiple sources into a unified relational database structure, and to visualize or query it using natural language.

Features
--------

* **Extraction:**

  * Seamlessly retrieve data from online data sources through web scraping, as well as from local files.
  * Support for various data formats, including ``.csv``, ``.xlsx``, ``.xls``, ``.txt``, ``.sav``, and compressed files.

* **Transformation:**

  * Consolidate extracted data into Dask DataFrames.
  * Optimize the transformation process for large files (parallel processing, efficient data structures).
  * Effectively manage data inconsistencies and discrepancies using anomaly detection algorithms.

* **Load:**

  * Consolidate transformed data into a cohesive relational database.

* **Query:**

  * Conduct precise queries and apply transformations to meet specific criteria.

* **AI Query & Visualization:**

  * Use natural language input to query data (from values to subsets).

Who should use socio4health?
----------------------------

**socio4health** is ideal for **data analysts**, **scientists**, and **researchers** who frequently handle large volumes of data from varied sources and are looking for a streamlined way to **consolidate**, **query**, and **visualize** their data. It is also a great tool for **developers** working on projects that require the integration of disparate data sets into a single, manageable format. **Business intelligence professionals** and **decision-makers** will find **socio4health** invaluable for generating insights through natural language queries and visualizations, making complex data more accessible and actionable. In essence, anyone looking to simplify their data workflows, from extraction to visualization, and leverage AI for natural language querying will benefit greatly from using **socio4health**.

About the Project
-----------------

Developed by `Erick Lozano <https://github.com/Ersebreck>`_, `Diego Irreño <https://github.com/dirreno>`_ and `Juan Montenegro <https://github.com/Juanmontenegro99>`_ © 2025

Documentation maintained by `Ingrid Mora <https://github.com/ingridvmoras>`_

License
-------

**socio4health** is distributed under the `MIT License <_MIT License: https://opensource.org/licenses/MIT>`_. Feel free to use, modify, and distribute this project in accordance with the terms of the license.

Contributing
------------

Contributions are warmly welcomed\! If you wish to contribute, please start a discussion about the proposed change before implementing it. More details can be found in our `GitHub repository <https://github.com/harmonize-tools/socio4health>`_.

Code of Conduct
---------------

**socio4health** is dedicated to fostering an inclusive community. We value the importance of creating a safe and welcoming environment for everyone. Please see our `Code of Conduct <https://github.com/harmonize-tools/socio4health/blob/main/CODE_OF_CONDUCT.md>`_.

----

.. _README: https://github.com/just-the-docs/just-the-docs-template/blob/main/README.md


.. toctree::
    :maxdepth: 2
    :hidden:

    User Guide <user_guide>
    Getting Started <getting_started>
    API Reference <API_reference>
    Examples <examples>
