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

from urllib3.util.retry import Retry as Urllib3Retry


class Retry(Urllib3Retry):
    """Retry configuration that extends `urllib3.util.retry` to support retry-after.

    This retry configuration class derives from
    `urllib3.util.retry.Retry
    <https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry>`_.

    Parameters
    ----------
    retry_after_status_codes : list
        The http status codes that should support the
        `Retry-After <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After>`_
        header.
    """

    DEFAULT_RETRY_AFTER_STATUS_CODES = frozenset([403, 413, 429, 503])

    def __init__(self, retry_after_status_codes=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if retry_after_status_codes is None:
            retry_after_status_codes = self.DEFAULT_RETRY_AFTER_STATUS_CODES

        if not isinstance(retry_after_status_codes, frozenset):
            retry_after_status_codes = frozenset(retry_after_status_codes)

        self.retry_after_status_codes = retry_after_status_codes

    def is_retry(self, method, status_code, has_retry_after=True):
        if not self._is_method_retryable(method):
            return False

        if self.status_forcelist and status_code in self.status_forcelist:
            return True

        return (
            self.total
            and self.respect_retry_after_header
            and has_retry_after
            and (status_code in self.retry_after_status_codes)
        )

    @classmethod
    def parse_retry_after_header(cls, retry_after):
        return Retry.parse_retry_after(cls, retry_after)
