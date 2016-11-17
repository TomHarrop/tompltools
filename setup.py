#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages

setup(
    name='tompltools',
    version='0.0.1',
    description='Tom\'s python3 functions for ruffus pipelines',
    url='https://github.com/TomHarrop/tompltools',
    author='Tom Harrop',
    author_email='twharrop@gmail.com',
    license='GPL-3',
    packages=find_packages(),
    install_requires=[
        'tompytools>=0.03',
        'datetime>=4.1.1'
        'os', 'subprocess', 're', 'tempfile'
    ],
    zip_safe=False)
