from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pylo',
    version='0.1.0',
    description='API Framwork and helpers for Illumio ASP platform',
    long_description=readme,
    author='Christophe Painchaud',
    author_email='christophe.painchaud@illumio.com',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs', 'pylo/vendors'))
)