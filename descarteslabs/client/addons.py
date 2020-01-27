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


class ThirdParty(object):
    _package = None

    def __init__(self, package):
        self._package = package

    def __getattr__(self, name):
        raise ImportError("Please install the %s package" % self._package)

    def __dir__(self):
        raise ImportError("Please install the %s package" % self._package)

    def __call__(self, *args, **kwargs):
        raise ImportError("Please install the %s package" % self._package)


try:
    import numpy
except ImportError:
    numpy = ThirdParty("numpy")

try:
    import blosc
except ImportError:
    blosc = ThirdParty("blosc")

try:
    import concurrent.futures
except ImportError:
    concurrent = ThirdParty("futures")

try:
    import mercantile
except ImportError:
    mercantile = ThirdParty("mercantile")


def import_matplotlib_pyplot():
    try:
        import matplotlib
    except ImportError:
        raise ImportError("The matplotlib package is required for displaying images.")
    try:
        import matplotlib.pyplot
    except RuntimeError as e:
        if matplotlib.get_backend() == "MacOSX":
            raise RuntimeError(
                "Python is not installed as a framework; the Mac OS X backend will not work.\n"
                "To resolve this, *before* calling dl.scenes.display(), execute this code:\n\n"
                "import matplotlib\n"
                "matplotlib.use('TkAgg')\n"
                "import matplotlib.pyplot as plt\n\n"
                "In an interactive session, you'll have to restart your Python interpreter first."
            )
        else:
            raise e
    return matplotlib
