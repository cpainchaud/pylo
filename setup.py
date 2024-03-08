from setuptools import setup, find_packages

import codecs
import os.path

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


__version__ = get_version("illumio_pylo/__init__.py")


setup_readme = "README.md"
setup_license = "LICENSE"

#setup get dependencies from requirements.txt
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='illumio_pylo',
    version=__version__,
    description='API Framework and Utilities for Illumio ASP platform',
    long_description=setup_readme,
    author='Christophe Painchaud',
    author_email='shellescape@gmail.com',
    url='https://github.com/cpainchaud/pylo',
    license=setup_license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=required,
    entry_points={
        'console_scripts': ['pylo-cli=pylo.cli:run'],
    },
    package_data={
        "": ["*.pem"],
    },
    python_requires='>=3.11',
    setup_requires=required
)