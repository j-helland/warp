from setuptools import setup

install_requires = [
    'warp', 
    'ipykernel',
    'matplotlib',
    'pydot']

setup(
    name='example', 
    packages=['example'], 
    install_requires=install_requires)
