#!/bin/env python
# coding: utf-8

import os
import re
import sys
from setuptools import setup, find_packages

PY_VER = sys.version_info
install_requires = []

if PY_VER >= (3, 4):
    pass
elif PY_VER >= (3, 3):
    install_requires.append('asyncio')
else:
    raise RuntimeError("aiomysql doesn't suppport Python earllier than 3.3")

def find_version(*file_paths):
    """
    read __init__.py
    """
    file_path = os.path.join(*file_paths)
    with open(file_path, 'r') as version_file:
        line = version_file.readline()
        while line:
            if line.startswith('__version__'):
                version_match = re.search(
                    r"^__version__ = ['\"]([^'\"]*)['\"]",
                    line,
                    re.M
                )
                if version_match:
                    return version_match.group(1)
            line = version_file.readline()
    raise RuntimeError("Unable to find version string.")

extras_require = {'sa': ['sqlalchemy>=0.9'], }

setup(
    name='aiosqlite3',
    version=find_version('aiosqlite3', '__init__.py'),
    packages=find_packages(),
    url='https://github.com/zeromake/aiosqlite3',
    license='MIT',
    author='zeromake',
    author_email='a390720046@gmail.com',
    description='sqlite3 support for asyncio.',
    platforms=['POSIX'],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        'Operating System :: POSIX',
        'Environment :: Web Environment',
        "Topic :: Database",
        'Framework :: AsyncIO',
    ],
    install_requires=install_requires,
    extras_require=extras_require
)
