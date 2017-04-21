from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='sfm',
    version='2.0.0',
    description='Merge three spotify feeds into one.',
    url='https://github.com/gahan-corporation/spotify-feed-merge',
    author='Gahan Corporation',
    author_email='info@gahan-corporation.com',
    license='BSD',

    py_modules=[
        "sfm.create_pipeline", 
        "sfm.options", 
        "sfm.streams", 
        "sfm.tracks", 
        "sfm.users", ],

    install_requires=['apache_beam', 'nose'],

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
