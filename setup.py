#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-bing-ads",
    version="2.0.9",
    description="Singer.io tap for extracting data from the Bing Ads API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_bingads"],
    install_requires=[
        "arrow==0.15.5",
        "bingads==13.0.15",
        "requests==2.22.0",
        "singer-python==5.8.1",
        "stringcase==1.2.0",
        "backoff==1.8.0",
        "xmltodict==0.12.0",
    ],
    extras_require={"dev": ["ipdb"]},
    entry_points="""
      [console_scripts]
      tap-bing-ads=tap_bing_ads:main
    """,
    packages=find_packages(),
)
