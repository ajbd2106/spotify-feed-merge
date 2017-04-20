from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='spotify_feed_merge',
    version='2.0.0',
    description='Merge three spotify feeds into one.',
    url='https://github.com/gahan-corporation/spotify-feed-merge',
    author='Gahan Corporation',
    author_email='info@gahan-corporation.com',
    license='BSD',

    packages=find_packages(),

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

    data_files=[
        ('streams', ['/srv/spotify-feed-merge/data/streams.gz']),
        ('tracks', ['/srv/spotify-feed-merge/data/tracks.gz']),
        ('users', ['/srv/spotify-feed-merge/data/users.gz']),
    ],

    entry_points={
        'console_scripts': [
            'merge=merge:main',
            'test=test:main',
        ],
    },
)
