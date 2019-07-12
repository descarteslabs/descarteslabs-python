#!/usr/bin/env python

# Copyright 2018-2019 Descartes Labs.
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
import ast
import os
import re
import sys

from setuptools import find_packages, setup

# Parse the docstring out of descarteslabs/__init__.py
_docstring_re = re.compile(r'"""((.|\n)*)\n"""', re.MULTILINE)
with open('descarteslabs/__init__.py', 'rb') as f:
    __doc__ = _docstring_re.search(f.read().decode('utf-8')).group(1)

DOCLINES = __doc__.split("\n")

# Parse version out of descarteslabs/client/version.py
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
        "geojson>=2.4.1",
        "shapely>=1.6.3,<2",
        "affine>=2.2.1",
        "pyarrow==0.13.0",
        "folium",
        "grpcio>=1.16.1,<2",
        "protobuf==3.8.0,<4",
    ]

    # Python < 2.7.9 needs requests[security] to avoid SSL issues
    # macOS ships with ancient OpenSSL which causes different SSL issues
    if sys.version_info[0:3] >= (2, 7, 9) and sys.platform != 'darwin':
        kwargs["install_requires"].append('requests>=2.20.1,<3')
    else:
        kwargs["install_requires"].append('requests[security]>=2.20.1,<3')

    kwargs['extras_require'] = {
        "complete": [
            'blosc;platform_system!="Windows"',
            "numpy>=1.10.0",
            "matplotlib>=2.1.0",
            "mercantile>=1.0.4",
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
