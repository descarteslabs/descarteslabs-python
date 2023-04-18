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
from http import HTTPStatus
from http.client import HTTPMessage
from unittest import mock

from requests import Session
from requests.adapters import HTTPAdapter

from ..retry import Retry


def mock_response(status, headers=None):
    if headers is None:
        headers = dict()

    msg = HTTPMessage()

    for key, value in headers.items():
        msg.add_header(key, value)

    return mock.Mock(status=status, msg=msg)


# unfortunately we cannot use responses here
# it does not support retries properly until 0.22.0
@mock.patch("urllib3.connectionpool.HTTPConnectionPool._put_conn")
@mock.patch("urllib3.connectionpool.HTTPConnectionPool._get_conn")
class TestRetry(unittest.TestCase):
    url = "https://example.com/some-service"

    def setUp(self):
        adapter = HTTPAdapter(max_retries=Retry(total=3))
        client = Session()
        client.mount("http://", adapter)
        client.mount("https://", adapter)

        self.client = client

    def test_retry_sets_status_codes(self, *mocks):
        retry = Retry()
        assert retry.retry_after_status_codes == Retry.DEFAULT_RETRY_AFTER_STATUS_CODES

        for code in Retry.DEFAULT_RETRY_AFTER_STATUS_CODES:
            assert retry.is_retry("GET", code, has_retry_after=True) is True
            assert retry.is_retry("GET", code, has_retry_after=False) is False

        retry = Retry(retry_after_status_codes=[])
        assert retry.retry_after_status_codes == frozenset([])
        assert retry.is_retry("GET", 403, has_retry_after=True) is False
        assert retry.is_retry("GET", 403, has_retry_after=False) is False

        retry = Retry(retry_after_status_codes=[400])
        assert retry.retry_after_status_codes == frozenset([400])
        assert retry.is_retry("GET", 403, has_retry_after=True) is False
        assert retry.is_retry("GET", 400, has_retry_after=True) is True
        assert retry.is_retry("GET", 400, has_retry_after=False) is False

    def test_retry_after_not_present(self, mock_conn, *mocks):
        mock_conn.return_value.getresponse.side_effect = [
            mock_response(status=HTTPStatus.FORBIDDEN)
        ]

        r = self.client.get(self.url)
        assert r.status_code == HTTPStatus.FORBIDDEN
        assert mock_conn.call_count == 1

    def test_retry_after_wrong_status(self, mock_conn, *mocks):
        mock_conn.return_value.getresponse.side_effect = [
            mock_response(
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                headers={"Retry-After": "1"},
            )
        ]

        r = self.client.get(self.url)
        assert r.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert mock_conn.call_count == 1

    def test_retry_after(self, mock_conn, *mocks):
        mock_conn.return_value.getresponse.side_effect = [
            mock_response(status=HTTPStatus.FORBIDDEN, headers={"Retry-After": "1"}),
            mock_response(status=HTTPStatus.OK),
        ]

        r = self.client.get(self.url)
        assert r.status_code == HTTPStatus.OK
        assert mock_conn.call_count == 2
