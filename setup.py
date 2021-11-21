from setuptools import setup, find_packages

install_requires = [
    'networkx',
    'pandas',
    'pyyaml',
    'rich',
    'dash',
    'visdcc',
]

setup(
    name="warp",
    version="0.0.0",
    license="All these functions are belong to the SEI",
    packages=find_packages(), 
    install_requires=install_requires)
