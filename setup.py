#! /usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'Justin Bayer, bayer.justin@googlemail.com'


from setuptools import setup, find_packages


setup(
    name="orakle",
    version="pre-0.1",
    description="machine learning via zeromq",
    license="BSD",
    keywords="Machine Learning Network",
    packages=find_packages(exclude=['examples', 'docs']),
    include_package_data=True,
)
