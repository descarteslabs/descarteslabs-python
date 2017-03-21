#!/usr/bin/env python
import sys
import os
from setuptools import setup, find_packages


def do_setup():
    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)

    from descarteslabs import __version__

    kwargs = {}
    kwargs['name'] = 'descarteslabs'
    kwargs['description'] = 'Descartes Labs Python Library'
    kwargs['long_description'] = open('README.md').read()
    kwargs['author'] = 'Descartes Labs'
    kwargs['author_email'] = 'hello@descarteslabs.com'
    kwargs['url'] = 'https://github.com/descarteslabs/descarteslabs'

    clssfrs = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
    ]
    kwargs['classifiers'] = clssfrs
    kwargs['version'] = __version__
    kwargs['packages'] = find_packages('.')
    kwargs['package_data'] = {'descarteslabs.services': ['gd_bundle-g2-g1.crt']}
    kwargs['scripts'] = [
        'descarteslabs/scripts/descarteslabs',
        'descarteslabs/scripts/waldo',
        'descarteslabs/scripts/raster',
        'descarteslabs/scripts/runcible',
    ]
    kwargs['zip_safe'] = False

    try:
        setup(**kwargs)
    finally:
        del sys.path[0]
        os.chdir(old_path)

    return


if __name__ == "__main__":
    do_setup()
