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
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
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
        python_requires="~=3.6",
        install_requires=[
            "affine>=2.2.2",
            "backports-datetime-fromisoformat>=1.0.0;python_version<'3.7'",
            "blosc==1.10.2",
            "cachetools>=3.1.1",
            "cloudpickle==0.4.0;python_version<'3.8'",
            "cloudpickle==1.6.0;python_version>='3.8'",
            "dataclasses>=0.8;python_version<'3.7'",
            "geojson>=2.5.0",
            "grpcio>=1.35.0,<2",
            "imagecodecs>=2020.5.30;python_version<'3.7'",
            "imagecodecs>=2021.5.20;python_version>='3.7'",
            "mercantile>=1.1.3",
            "numpy>=1.18.1",
            "Pillow>=8.1.1",
            "protobuf>=3.14.0,<4",
            "pyarrow>=3.0.0",
            "pytz>=2021.1",
            "requests[security]>=2.25.1,<3",
            "six>=1.15.0",
            "shapely>=1.7.1,<2",
            "tifffile==2020.9.3;python_version<'3.7'",
            "tifffile==2021.4.8;python_version>='3.7'",
            "tqdm>=4.32.1",
        ],
        extras_require={
            "complete": [
                "matplotlib>=3.1.2",
                "ipyleaflet>=0.13.3,<1",
                "ipywidgets>=7.5.1,<8",
                "traitlets>=4.3.3,<6;python_version<'3.7'",
                "traitlets==5.0.5,<6;python_version>='3.7'",
                "markdown2>=2.4.0,<3",
            ],
            "tests": [
                "hypothesis[numpy]==5.7.0",
                "mock",
                "pytest==6.0.0",
                "responses==0.12.1",
                "freezegun==0.3.12",
            ],
        },
        data_files=[("docs/descarteslabs", ["README.md"])],
    )


if __name__ == "__main__":
    check_setuptools()
    do_setup()
