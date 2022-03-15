from setuptools import setup, find_packages

setup(
    name='ihub_data_loader',
    version='1.0.0',
    packages=find_packages(),
    url='http://nexusgit/bsm/mapes-i-rutes-smou-p1/smou-maps-mapbox-data-upload',
    license='-',
    author='Nexus Geographics',
    author_email='info@nexusgeograhics.com',
    description='Tool to load data from diferent sources to ihub consumer',
    install_requires=[
        "pyyaml",
        "SQLAlchemy",
        "psycopg2",
        "pandas",
        "requests",
        "pyodbc"
    ]
)
