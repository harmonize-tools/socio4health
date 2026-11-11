"""Meta-paquete ``socio4health``.

No contiene codigo: solo depende de ``socio4health-core[all]`` en la misma
version, para que ``pip install socio4health`` instale todo (scraping, ml, geo).

Para una instalacion basica o selectiva usar ``socio4health-core``:
    pip install socio4health-core
    pip install "socio4health-core[geo]"
"""

import re
from os import path
from setuptools import setup

here = path.abspath(path.dirname(__file__))


def get_version():
    # Desde el repositorio: misma version que el paquete core.
    init_file = path.join(here, '..', 'src', 'socio4health', '__init__.py')
    if path.exists(init_file):
        with open(init_file, encoding='utf-8') as f:
            return re.search(r'^__version__\s*=\s*[\'"]([^\'"]+)[\'"]', f.read(), re.M).group(1)
    # Desde un sdist ya construido (el __init__ no esta incluido): usar PKG-INFO.
    with open(path.join(here, 'PKG-INFO'), encoding='utf-8') as f:
        return re.search(r'^Version:\s*(\S+)', f.read(), re.M).group(1)


version = get_version()

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='socio4health',
    version=version,
    description='Socio4health is a Python package for gathering and consolidating socio-demographic data.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/harmonize-tools/socio4health',
    author='Erick Lozano, Diego Irreño, Juan Montenegro, Ingrid Mora',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
    ],
    python_requires='>=3.10, <4',
    packages=[],
    install_requires=[
        f'socio4health-core[all]=={version}',
    ],
    project_urls={
        'Bug Reports': 'https://github.com/harmonize-tools/socio4health/issues',
        'Source': 'https://github.com/harmonize-tools/socio4health/',
        'Changelog': 'https://github.com/harmonize-tools/socio4health/blob/main/CHANGELOG.md',
    },
)
