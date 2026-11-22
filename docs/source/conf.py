# Configuration file for the Sphinx documentation builder.
import os
import shutil
import sys
from pathlib import Path


sys.path.insert(0, os.path.abspath('../../src'))

# The example notebooks live in <repo>/notebooks. Sphinx only reads files under docs/source, so they are copied
# to docs/source/notebooks (git-ignored) for each build and removed when it finishes; their pages keep the URL
# notebooks/<name>.html.
_NOTEBOOKS = Path(__file__).resolve().parents[2] / "notebooks"
_NOTEBOOK_COPIES = Path(__file__).resolve().parent / "notebooks"
_NOTEBOOK_COPIES.mkdir(exist_ok=True)
for _copy in _NOTEBOOK_COPIES.glob("*.ipynb"):
    if not (_NOTEBOOKS / _copy.name).exists():   # notebook removed or renamed
        _copy.unlink()
for _notebook in _NOTEBOOKS.glob("*.ipynb"):
    shutil.copy2(_notebook, _NOTEBOOK_COPIES / _notebook.name)
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
    'myst_nb',
    'sphinx_design',
    'nbsphinx',
    'sphinx_copybutton'

]

nbsphinx_execute = 'never'

templates_path = ['_templates']
exclude_patterns = ['_build', '**/.ipynb_checkpoints']
autosummary_generate = True
nb_execution_mode = "off"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_title = "socio4health"
html_theme = "pydata_sphinx_theme"
html_static_path = ['_static']
html_theme_options = {
    "logo": {
        "image_light": "_static/image.png",  # Asegúrate de que la imagen esté en docs/source/_static/
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
    # None lets Sphinx decide; returning `skip` (False from autosummary) would force private members in
    return None

def remove_notebook_copies(app, exception):
    # The copies are only needed while Sphinx reads the sources
    shutil.rmtree(_NOTEBOOK_COPIES, ignore_errors=True)


def setup(app):
    app.connect("autodoc-skip-member", skip_property)
    app.connect("build-finished", remove_notebook_copies)