from setuptools import setup, find_packages

def readme():
    with open("README.md", 'r') as f:
        return f.read()

setup(
    name = "ingress",
    description = "The ingress point for a digital repository",
    long_description = readme(),
    packages = find_packages(
        exclude = [
        ]
    ),
    dependency_links = [
        'https://github.com/uchicago-library/uchicagoldr-premiswork' +
        '/tarball/master#egg=pypremis'
    ],
    install_requires = [
        'flask>0',
        'flask_restful',
        'pypremis'
    ],
)
