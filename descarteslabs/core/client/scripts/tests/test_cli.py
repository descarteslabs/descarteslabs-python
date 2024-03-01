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

import unittest

import click.testing

from ...version import __version__
from ..cli import cli


class TestCli(unittest.TestCase):
    def setUp(self):
        self.runner = click.testing.CliRunner()

    def test_version(self):
        result = self.runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert result.output == f"{__version__}\n"

    def test_env(self):
        result = self.runner.invoke(cli, ["env"])
        assert result.exit_code == 0
        assert result.output == "testing\n"
