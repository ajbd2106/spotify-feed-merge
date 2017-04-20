from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='spotify-feed-merge',
    version='2.0.0',
    description='Merge three spotify feeds into one.'
    url='https://github.com/gahan-corporation/spotify-feed-merge',
    author='Gahan Corporation',
    author_email='info@gahan-corporation.com',
    license='BSD',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    install_requires=['apache_beam', 'nose'],

    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    package_data={
        'sfm': ['sfm.conf'],
    },

    data_files=[('my_data', ['data/data_file'])],

    entry_points={
        'console_scripts': [
            'merge=merge:main',
            'test=test:main',
        ],
    },
)
