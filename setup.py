from setuptools import setup, find_packages

setup(
    name='eloverblik',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'pandas',
        'duckdb'
    ],
    entry_points={
        'console_scripts': [
            'databuild = eloverblik.scripts.eldata:databuild',
        ],
    },
)
