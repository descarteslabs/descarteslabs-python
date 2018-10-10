#!/usr/bin/env python

# Copyright 2018 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Descartes Labs Python Client

.. code-block:: bash

    pip install descarteslabs[complete]

Documentation is available at https://docs.descarteslabs.com.

Source code and version information is at https://github.com/descarteslabs/descarteslabs-python.

The Descartes Labs Platform simplifies analysis of **global-scale raster data** by providing:

  * Access to a catalog of petabytes of disparate geospatial data,
    all normalized and interoperable through one **common interface**
  * **Infrastructure** to parallelize any code across thousands of machines co-located with that data
  * The ability to **add new data to that catalog**-whether the output of analysis on existing data,
    or from a proprietary source-which can then be used as an input for more analysis
  * A Python client library to access these systems
  * Web interfaces to `browse this catalog <https://catalog.descarteslabs.com/>`_
    and `view imagery <https://viewer.descarteslabs.com/>`_, including your data you create
"""

import ast
import os
import re
import sys

from setuptools import find_packages, setup

DOCLINES = __doc__.split("\n")

# Parse version out of descarteslabs/__init__.py
_version_re = re.compile(r'__version__\s+=\s+(.*)')
with open('descarteslabs/client/version.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(f.read().decode('utf-8')).group(1)))


def check_setuptools():
    import pkg_resources
    try:
        list(pkg_resources.parse_requirements('foo;platform_system!="Windows"'))
    except pkg_resources.RequirementParseError:
        exit('Your Python is using an outdated version of `setuptools`. Please '
             'run `pip install -U setuptools` and try again.')


def do_setup():
    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)

    kwargs = {}
    kwargs['name'] = 'descarteslabs'
    kwargs['description'] = DOCLINES[0]
    kwargs['long_description'] = "\n".join(DOCLINES[2:])
    kwargs['author'] = 'Descartes Labs'
    kwargs['author_email'] = 'hello@descarteslabs.com'
    kwargs['url'] = 'https://github.com/descarteslabs/descarteslabs-python'
    kwargs['download_url'] = "https://github.com/descarteslabs/descarteslabs-python/archive/v%s.tar.gz" % version

    clssfrs = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ]
    kwargs['classifiers'] = clssfrs
    kwargs['version'] = version
    kwargs['packages'] = find_packages('.')
    kwargs['entry_points'] = {
        'console_scripts': [
            'descarteslabs = descarteslabs.client.scripts.__main__:main',
        ],
    }
    kwargs['install_requires'] = [
        "cloudpickle==0.4.0",
        "six",
        "cachetools>=2.0.1",
        'futures;python_version=="2.7"',
        "geojson>=2.4.0",
        "shapely>=1.6.3,<2",
    ]

    # Python < 2.7.9 needs requests[security] to avoid SSL issues
    # macOS ships with ancient OpenSSL which causes different SSL issues
    if sys.version_info[0:3] >= (2, 7, 9) and sys.platform != 'darwin':
        kwargs["install_requires"].append('requests>=2.16.0')
    else:
        kwargs["install_requires"].append('requests[security]>=2.16.0')

    kwargs['extras_require'] = {
        "complete": [
            'blosc;platform_system!="Windows"',
            "numpy>=1.10.0",
            "matplotlib>=2.1.0",
        ],
    }
    kwargs['license'] = 'Apache 2.0'
    kwargs['zip_safe'] = False

    try:
        setup(**kwargs)
    finally:
        del sys.path[0]
        os.chdir(old_path)


if __name__ == "__main__":
    check_setuptools()
    do_setup()
