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

# normally this will be used from the public client and will not be pre-configured
try:
    from descarteslabs._dl_modules.client.scripts.cli import parser, handle
except ImportError:
    # run from monorepo, somewhat unusual
    from descarteslabs.client.scripts.cli import parser, handle


def main():
    try:
        handle(parser.parse_args())
    except Exception as e:
        print("{}: {}".format(e.__class__.__name__, e))


if __name__ == "__main__":
    main()
