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
    """Retry configuration that allows configuration of retry-after support.

    This retry configuration class derives from
    `urllib3.util.retry.Retry
    <https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry>`_.

    Parameters
    ----------
    retry_after_status_codes : list
        The http status codes that should support the
        `Retry-After <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After>`_
        header. This is in lieu of the hardwired urllib3 Retry.RETRY_AFTER_STATUS_CODES.
    """

    DEFAULT_RETRY_AFTER_STATUS_CODES = frozenset([403, 413, 429, 503])

    def __init__(self, *args, retry_after_status_codes=None, **kwargs):
        super().__init__(*args, **kwargs)

        if retry_after_status_codes is None:
            retry_after_status_codes = self.DEFAULT_RETRY_AFTER_STATUS_CODES

        if not isinstance(retry_after_status_codes, frozenset):
            retry_after_status_codes = frozenset(retry_after_status_codes)

        # Overrides the urllib3.util.retry.Retry.RETRY_AFTER_STATUS_CODES
        # class variable.
        self.RETRY_AFTER_STATUS_CODES = retry_after_status_codes
