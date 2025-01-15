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

import unittest

from ..retry import Retry


class TestRetry(unittest.TestCase):
    def test_retry_sets_status_codes(self, *mocks):
        retry = Retry()
        assert retry.RETRY_AFTER_STATUS_CODES == Retry.DEFAULT_RETRY_AFTER_STATUS_CODES

        for code in Retry.DEFAULT_RETRY_AFTER_STATUS_CODES:
            assert retry.is_retry("GET", code, has_retry_after=True) is True
            assert retry.is_retry("GET", code, has_retry_after=False) is False

        retry = Retry(retry_after_status_codes=[])
        assert retry.RETRY_AFTER_STATUS_CODES == frozenset([])
        assert retry.is_retry("GET", 403, has_retry_after=True) is False
        assert retry.is_retry("GET", 403, has_retry_after=False) is False

        retry = Retry(retry_after_status_codes=[400])
        assert retry.RETRY_AFTER_STATUS_CODES == frozenset([400])
        assert retry.is_retry("GET", 403, has_retry_after=True) is False
        assert retry.is_retry("GET", 400, has_retry_after=True) is True
        assert retry.is_retry("GET", 400, has_retry_after=False) is False
