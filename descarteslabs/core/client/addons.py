# Copyright 2018-2023 Descartes Labs.
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
