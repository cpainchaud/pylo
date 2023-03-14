from setuptools import setup, find_packages

#
# with open('README.rst') as f:
#     readme = f.read()
#
# with open('LICENSE') as f:
#     license = f.read()

setup_readme = "TODO"
setup_license = "TODO"

setup(
    name='pylo',
    version='0.2.4',
    description='API Framework and Utilities for Illumio ASP platform',
    long_description=setup_readme,
    author='Christophe Painchaud',
    author_email='shellescape@gmail.com',
    url='',
    license=setup_license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'requests~=2.25.1',
        'openpyxl~=3.0.6',
        'xlsxwriter~=1.3.7',
        'colorama~=0.4.4',
    ],
    entry_points={
        'console_scripts': ['pylo-cli=pylo.cli:run'],
    },
    package_data={
        "": ["*.pem"],
    },
)