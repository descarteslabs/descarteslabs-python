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
from unittest import mock

from ..attributes import Attribute
from ..document import Document
from ..search import Search


class DocumentTest(Document):
    name = Attribute(str, filterable=True)
    num = Attribute(int, sortable=True)
    order = Attribute(int, sortable=True)


class TestSearch(unittest.TestCase):
    def test_search_immutable(self):
        mock_client = mock.Mock()
        search = Search(DocumentTest, mock_client, "/test_url")
        search.filter(DocumentTest.name == "test")
        search.sort(DocumentTest.order, -DocumentTest.num)
        search.param(test="blah")
        search.limit(10)
        assert search._serialize() == {}

    def test_build_search(self):
        mock_client = mock.Mock()
        search = Search(DocumentTest, mock_client, "/test_url")
        search = (
            search.filter(DocumentTest.name == "test")
            .sort(DocumentTest.order, -DocumentTest.num)
            .param(test="blah")
            .limit(10)
        )
        assert search._serialize() == {
            "filter": '[{"name":"name","op":"eq","val":"test"}]',
            "sort": ["order", "-num"],
            "limit": 10,
            "test": "blah",
        }

    def test_search_not_allowed(self):
        mock_client = mock.Mock()
        search = Search(DocumentTest, mock_client, "/test_url")

        with self.assertRaises(ValueError) as ctx:
            search.filter(DocumentTest.num == 123)
        assert "Cannot filter on property: num" in str(ctx.exception)

        with self.assertRaises(ValueError) as ctx:
            search.sort(DocumentTest.name)
        assert "Cannot sort on property: name" in str(ctx.exception)
