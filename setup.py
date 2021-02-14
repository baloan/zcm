#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from pathlib import Path

from setuptools import setup


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    version = Path(package, "__version__.py").read_text()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", version).group(1)


def get_long_description():
    """
    Return the README.
    """
    long_description = ""
    with open("README.md", encoding="utf8") as f:
        long_description += f.read()
    long_description += "\n\n"
    with open("CHANGELOG.md", encoding="utf8") as f:
        long_description += f.read()
    return long_description


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [str(path.parent) for path in Path(package).glob("**/__init__.py")]


setup(
    name="zcm",
    python_requires=">=3.6",
    version=get_version("zcm"),
    url="https://github.com/baloan/zcm",
    project_urls={
        "Changelog": "https://github.com/baloan/zcm/CHANGELOG.md",
        "Documentation": "https://...",
        "Source": "https://github.com/baloan/zcm",
    },
    license="MIT",
    description="ZODB context manager",
    author="Andreas Balogh",
    author_email="baloand@gmail.com",
    zip_safe=False,
    install_requires=[
        "ZODB",
    ],
    extras_require={
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Internet :: WWW/HTTP",
        "Framework :: AsyncIO",
        "Framework :: Trio",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
