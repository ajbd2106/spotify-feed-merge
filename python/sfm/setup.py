from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='sfm',
    version='0.2.0',
    description='Merge three spotify feeds into one.',
    url='https://github.com/gahan-corporation/spotify-feed-merge',
    author='Gahan Corporation',
    author_email='info@gahan-corporation.com',
    license='BSD',
    install_requires=['apache_beam', 'configobj'],
    package_data={
        'sfm': ['sfm.conf'],
    },
    data_files=[
        ('streams.gz', ['/srv/sfm/data/streams.gz']),
        ('tracks.gz', ['/srv/sfm/data/tracks.gz']),
        ('users.gz', ['/srv/sfm/data/users.gz']),
    ],
    test_suite='nose.collector',
    tests_require=['nose']
)
