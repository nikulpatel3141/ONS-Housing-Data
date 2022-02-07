from setuptools import setup, find_packages

setup(
    name="sampleproject",
    version="0.0",
    description="Helper functions for loading, processing and analysing ONS data",
    author="Nikul Patel",  # Optional
    packages=find_packages(),
    install_requires=["peppercorn"],
)
