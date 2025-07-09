# Configuration file for the Sphinx documentation builder.
import os
import sys


sys.path.insert(0, os.path.abspath('../../src'))
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'socio4health'
copyright = '2025, Harmonize'
author = 'Harmonize'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.apidoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.githubpages',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.todo',
    'myst_nb'
    #'sphinx.ext.autosectionlabel'

]

#autosectionlabel_prefix_document = True

templates_path = ['_templates']
exclude_patterns = []
autosummary_generate = True


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_title = "socio4health"
html_theme = "pydata_sphinx_theme"
html_static_path = ['_static']
html_theme_options = {
    "logo": {
        "image_light": "img/image.png",  # Asegúrate de que la imagen esté en docs/source/_static/
        "text": "socio4health"
    },
    "icon_links": [
        {
            # Label for this link
            "name": "GitHub",
            "url": "https://github.com/harmonize-tools/socio4health",
            "icon": "fa-brands fa-github",
            "type": "fontawesome",
        }]
}


intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pandas': ('https://pandas.pydata.org/docs', None),
}

#exclude_patterns = [
   # 'socio4health.dict.rst',
   # 'socio4health.enums.rst',
    #'socio4health.utils.rst',
#]

def skip_property(app, what, name, obj, skip, options):
    if isinstance(obj, property):
        return True
    return skip

def setup(app):
    app.connect("autodoc-skip-member", skip_property)