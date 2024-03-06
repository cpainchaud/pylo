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
    version='0.2.5',
    description='API Framework and Utilities for Illumio ASP platform',
    long_description=setup_readme,
    author='Christophe Painchaud',
    author_email='shellescape@gmail.com',
    url='',
    license=setup_license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'click~=8.1.7',
        'colorama~=0.4.4',
        'cryptography~=42.0.5',
        'openpyxl~=3.0.10',
        'paramiko~=3.4.0',
        'prettytable~=3.10.0'
        'requests~=2.31.0',
        'xlsxwriter~=1.3.7',
    ],
    entry_points={
        'console_scripts': ['pylo-cli=pylo.cli:run'],
    },
    package_data={
        "": ["*.pem"],
    },
    python_requires='>=3.11',
)