# Copyright 2018-2024 Descartes Labs.
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

from ..cli import cli


class TestCli(unittest.TestCase):
    def setUp(self):
        self.runner = click.testing.CliRunner()

    def test_help(self):
        result = self.runner.invoke(cli, [])
        assert result.exit_code == 0
        assert result.output.startswith("Usage: ")

    # at present, I don't want to test individual commands,
    # it'd be a huge pain to mock and would basically just be
    # testing catalog itself.
    def test_products(self):
        result = self.runner.invoke(cli, ["products"])
        assert result.exit_code == 0
        assert result.output.startswith("Usage: ")

    def test_bands(self):
        result = self.runner.invoke(cli, ["bands"])
        assert result.exit_code == 0
        assert result.output.startswith("Usage: ")

    def test_blobs(self):
        result = self.runner.invoke(cli, ["blobs"])
        assert result.exit_code == 0
        assert result.output.startswith("Usage: ")
