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
    entry_points = {
        'console_scripts': ['pylo-cli=pylo.utilities.cli'],
    }
)