#!/usr/bin/env python

# Copyright 2017 Descartes Labs.
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

import sys
import os
from setuptools import setup, find_packages


__version__ = "0.2.0"


def do_setup():
    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)

    kwargs = {}
    kwargs['name'] = 'descarteslabs'
    kwargs['description'] = 'Descartes Labs Python Library'
    kwargs['long_description'] = open('README.md').read()
    kwargs['author'] = 'Descartes Labs'
    kwargs['author_email'] = 'hello@descarteslabs.com'
    kwargs['url'] = 'https://github.com/descarteslabs/descarteslabs-python'
    kwargs['download_url'] = "https://github.com/descarteslabs/descarteslabs-python/archive/v%s.tar.gz" % __version__

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
    kwargs['version'] = __version__
    kwargs['packages'] = find_packages('.')
    kwargs['package_data'] = {'descarteslabs.services': ['gd_bundle-g2-g1.crt']}
    kwargs['scripts'] = [
        'descarteslabs/scripts/descarteslabs',
        'descarteslabs/scripts/places',
        'descarteslabs/scripts/raster',
        'descarteslabs/scripts/metadata',
    ]
    kwargs['install_requires'] = [
        "requests",
        "cachetools",
        "six",
    ]
    kwargs['license'] = 'Apache 2.0',
    kwargs['zip_safe'] = False

    try:
        setup(**kwargs)
    finally:
        del sys.path[0]
        os.chdir(old_path)

    return


if __name__ == "__main__":
    do_setup()
