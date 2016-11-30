#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages

setup(
    name='tompltools',
    version='0.0.7',
    description='Tom\'s python3 functions for ruffus pipelines',
    url='https://github.com/TomHarrop/tompltools',
    author='Tom Harrop',
    author_email='twharrop@gmail.com',
    license='GPL-3',
    packages=find_packages(),
    install_requires=[
        'tompytools',
    ],
    zip_safe=False)
