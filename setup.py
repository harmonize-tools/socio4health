"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

from setuptools import setup
from os import path
from glob import glob


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Optionally append the changelog to the long description so PyPI shows it
changelog_path = path.join(here, 'CHANGELOG.md')
try:
    with open(changelog_path, encoding='utf-8') as f:
        changelog = f.read()
    # Separate README and changelog with a horizontal rule for readability
    long_description = long_description + "\n\n---\n\n" + changelog
except FileNotFoundError:
    # If changelog is missing, continue without it
    changelog = None

setup( 
    name='socio4health',
    version='1.0.8',
    description='Socio4health is a Python package for gathering and consolidating socio-demographic data.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/harmonize-tools/socio4health',
    author='Erick Lozano, Diego Irreño, Juan Montenegro, Ingrid Mora',
    author_email='',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='extract transform load etl scraping relational census sociodemographic colombia brazil',
    package_dir={'': 'src'},
    packages=['socio4health', 'socio4health.enums', 'socio4health.utils'],
    python_requires='>=3.10, <4',

    # DEPENDENCIAS ESENCIALES - Solo lo mínimo para que funcione el ETL básico
    install_requires=[
        "pandas>=2.0.0",
        "requests>=2.31.0",
        "tqdm>=4.66.1",
        "pyreadstat>=1.2.6",
        "py7zr>=0.20.8",
        "openpyxl>=3.1.2",
        "appdirs>=1.4.4",
        "pyarrow>=12.0.0",
        "pyzipper==0.3.6",
        "numpy>=1.24.0",
        "dask>=2023.0.0"
    ],

    # DEPENDENCIAS OPCIONALES - Organizadas por funcionalidad
    extras_require={
        # Scraping web
        'scraping': [
            'Scrapy>=2.11.1',
        ],
        
        # Machine Learning y NLP
        'ml': [
            'transformers>=4.30.0',
            'torch>=2.0.0',
            'torchaudio>=2.0.0',
            'torchvision>=0.15.0',
            'deep-translator>=1.11.4'
        ],
        
        # Análisis geoespacial
        'geo': [
            'geopandas>=0.14.0',
        ],
        
        # ALL
        'all': [
            'socio4health[scraping,ml,geo]',
        ],
        
        # Desarrollo y testing
        'dev': [
            'check-manifest',
            'pytest',
            'black',
            'flake8',
        ],
        'test': [
            'coverage',
            'pytest-cov',
        ],
    },

    package_data={
        'socio4health': ['src/socio4health/config/*.json'],
    },

    data_files=[('harmonize_data', glob('src/socio4health/config/*.json'))],

    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },

    project_urls={
        'Bug Reports': 'https://github.com/harmonize-tools/socio4health/issues',
        'Source': 'https://github.com/harmonize-tools/socio4health/',
        'Changelog': 'https://github.com/harmonize-tools/socio4health/blob/main/CHANGELOG.md',
    },
)