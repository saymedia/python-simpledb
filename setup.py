#!/usr/bin/env python
import os
from distutils.core import setup
setup(
    name='simpledb',
    version='1.0',
    description='Python SimpleDB API SDK',
    long_description = open(os.path.join(os.path.dirname(__file__), 'README')).read(),
    author='Michael Malone',
    author_email='mjmalone@gmail.com',
    url='http://github.com/mmalone/python-simpledb',

    packages=['simpledb'],
    provides=['simpledb'],
    requires=[
        'httplib2',
        'elementtree',
    ]
)
