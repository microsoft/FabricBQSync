from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='FabricSync',
    version='1.1.0',
    author='Microsoft GBBs North America',  
    author_email='chriprice@microsoft.com',
    description='Fabric Data Sync Utility',
    packages=find_packages(),
    install_requires=requirements
)
