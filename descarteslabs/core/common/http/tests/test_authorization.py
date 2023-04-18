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

from .. import authorization


class TestAuthorization(unittest.TestCase):
    def test_add_bearer(self):
        assert authorization.add_bearer("foo") == "Bearer foo"
        assert authorization.add_bearer("foo") == "Bearer foo"
        assert authorization.add_bearer(b"foo") == b"Bearer foo"

    def test_remove_bearer(self):
        assert authorization.remove_bearer("Bearer foo") == "foo"
        assert authorization.remove_bearer("Bearer foo") == "foo"
        assert authorization.remove_bearer(b"Bearer foo") == b"foo"
