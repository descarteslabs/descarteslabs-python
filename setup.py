#!/usr/bin/env python

# © 2025 EarthDaily Analytics Corp.
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

from setuptools import find_packages, setup

# Parse the docstring out of descarteslabs/__init__.py
_docstring_re = re.compile(r'"""((.|\n)*)\n"""', re.MULTILINE)
with open("descarteslabs/__init__.py", "rb") as f:
    __doc__ = _docstring_re.search(f.read().decode("utf-8")).group(1)

DOCLINES = __doc__.split("\n")

# Parse version out of descarteslabs/core/client/version.py
_version_re = re.compile(r"__version__\s+=\s+(.*)")
with open("descarteslabs/core/client/version.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )


def do_setup():
    viz_requires = [
        "matplotlib>=3.1.2",
        "ipyleaflet>=0.17.2",
    ]
    tests_requires = [
        "pytest==6.0.0",
        "responses==0.12.1",
        "freezegun==0.3.12",
    ]
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
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
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
                "config/settings.toml",
            ]
        },
        include_package_data=True,
        entry_points={
            "console_scripts": [
                "descarteslabs = descarteslabs.core.client.scripts.__main__:main"
            ]
        },
        python_requires="~=3.9",
        install_requires=[
            "affine>=2.2.2",
            "blosc>=1.11.2",
            "cachetools>=3.1.1",
            "click>=8.2.0",
            "dill>=0.3.6",
            "dynaconf>=3.2.1",
            "geojson>=2.5.0",
            "geopandas>=0.13.2",
            "imagecodecs>=2023.3.16",
            "lazy_object_proxy>=1.7.1",
            "mercantile>=1.1.3",
            "numpy>=1.22.0;python_version>='3.9' and python_version<'3.11'",
            "numpy>=1.23.2;python_version>='3.11'",
            "packaging>=25.0",
            "Pillow>=9.2.0",
            "pyarrow>=14.0.1",
            "pydantic>=2.4.0",
            "requests>=2.32.3,<3",
            "shapely>=2.0.0",
            "strenum>=0.4.8",
            "tifffile>=2023.9.26",
            "tqdm>=4.66.3",
            "urllib3>=1.26.19, !=2.0.0, !=2.0.1, !=2.0.2, !=2.0.3, !=2.0.4",
        ],
        extras_require={
            "visualization": viz_requires,
            "complete": viz_requires,
            "tests": tests_requires,
        },
        data_files=[("docs/descarteslabs", ["README.md"])],
    )


if __name__ == "__main__":
    do_setup()
