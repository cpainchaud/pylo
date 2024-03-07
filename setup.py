from setuptools import setup, find_packages

__version__ = "0.2.5"


setup_readme = "TODO"
setup_license = "TODO"

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
    url='https://github.com/cpainchaud/illumio_pylo',
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