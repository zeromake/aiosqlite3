#!/bin/env python
# coding: utf-8

"""

"""
import os
import re
from setuptools import setup, find_packages


def find_version(*file_paths):
    """
    read __init__.py
    """
    file_path = os.path.join(file_paths)
    with open(file_path, 'r') as version_file:
        line = version_file.readline()
        if line.startswith('__version__'):
            version_match = re.search(
                r"^__version__ = ['\"]([^'\"]*)['\"]",
                line,
                re.M
            )
            if version_match:
                return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name='aiosqlite3',
    version=find_version('aiosqlite3', '__init__.py'),
    packages=find_packages(),
    url='https://github.com/zeromake/aiosqlite3',
    license='MIT',
    author='zeromake',
    author_email='a390720046@gmail.com',
    description='sqlite3 support for asyncio.',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: System :: Filesystems",
    ]
)
