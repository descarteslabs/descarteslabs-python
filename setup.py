#!/usr/bin/env python

# Copyright 2018-2020 Descartes Labs.
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
import re
import sys

from setuptools import find_packages, setup

# Parse the docstring out of descarteslabs/__init__.py
_docstring_re = re.compile(r'"""((.|\n)*)\n"""', re.MULTILINE)
with open("descarteslabs/__init__.py", "rb") as f:
    __doc__ = _docstring_re.search(f.read().decode("utf-8")).group(1)

DOCLINES = __doc__.split("\n")

# Parse version out of descarteslabs/client/version.py
_version_re = re.compile(r"__version__\s+=\s+(.*)")
with open("descarteslabs/client/version.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )


def check_setuptools():
    import pkg_resources

    try:
        list(pkg_resources.parse_requirements('foo;platform_system!="Windows"'))
    except pkg_resources.RequirementParseError:
        sys.exit(
            "Your Python is using an outdated version of `setuptools`. Please "
            "run `pip install -U setuptools` and try again."
        )


def do_setup():
    setup(
        name="descarteslabs",
        description=DOCLINES[0],
        long_description="\n".join(DOCLINES[2:]),
        author="Descartes Labs",
        author_email="hello@descarteslabs.com",
        url="https://github.com/descarteslabs/descarteslabs-python",
        classifiers=[
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.5",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
        ],
        license="Apache 2.0",
        download_url=(
            "https://github.com/descarteslabs/descarteslabs-python/archive/v{}.tar.gz".format(
                version
            )
        ),
        version=version,
        packages=find_packages(),
        package_data={
            "descarteslabs": [
                "client/services/tasks/tests/data/dl_test_package/package/*.pyx",
                "client/services/tasks/tests/data/dl_test_package/*.json",
                "client/services/tasks/tests/data/*.txt",
            ]
        },
        include_package_data=True,
        entry_points={
            "console_scripts": [
                "descarteslabs = descarteslabs.client.scripts.__main__:main"
            ]
        },
        python_requires="~=3.5",
        install_requires=[
            "affine>=2.2.1",
            "backports-datetime-fromisoformat>=1.0.0;python_version<'3.7'",
            "cachetools>=2.0.1",
            "cloudpickle==0.4.0",
            "geojson>=2.5.0",
            "grpcio>=1.16.1,<2",
            "protobuf==3.11.2,<4",
            "numpy>=1.17.4",
            "pyarrow==0.17.1",
            "pytz>=2019.1",
            "requests[security]>=2.20.1,<3",
            "six>=1.12.0",
            "shapely>=1.6.3,<2",
        ],
        extras_require={
            "complete": [
                'blosc==1.8.3;platform_system!="Windows"',
                "matplotlib>=3.0.3",
                "mercantile>=1.1.3",
                "ipyleaflet>=0.13.1,<1",
                "ipywidgets>=7.5.1,<8",
                "traitlets>=4.3.3,<5",
                "markdown2>=2.3.9,<3",
            ],
            "tests": [
                "hypothesis[numpy]==5.7.0",
                "mock",
                "pytest==4.6.3",
                "responses",
                "freezegun",
            ],
        },
        data_files=[("docs/descarteslabs", ["README.md"])],
    )


if __name__ == "__main__":
    check_setuptools()
    do_setup()
