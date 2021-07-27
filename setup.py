from setuptools import setup, find_packages

#
# with open('README.rst') as f:
#     readme = f.read()
#
# with open('LICENSE') as f:
#     license = f.read()

readme = "TODO"
licence = "TODO"

setup(
    name='pylo',
    version='0.2.0',
    description='API Framework and Utilities for Illumio ASP platform',
    long_description=readme,
    author='Christophe Painchaud',
    author_email='shellescape@gmail.com',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs', 'pylo/vendors')),
    install_requires=[
        'requests~=2.25.1',
        'openpyxl~=3.0.6',
        'xlsxwriter~=1.3.7',
    ],
    entry_points={
        'console_scripts': ['pylo-cli=pylo.utilities.cli'],
    },
)