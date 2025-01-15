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

from descarteslabs.auth import Auth
from descarteslabs.config import get_settings

from ..client.services.service import ApiService
from ..common.http.service import DefaultClientMixin


class VectorClient(ApiService, DefaultClientMixin):
    """Client for the Vector service."""

    # We need a long timeout until we rewrite uploading and downloading
    # features to work in chunks. Note that this is not applied by default
    # to the session (which has a 30 second read timeout), but we use it
    # where we need it.
    READ_TIMEOUT = 300

    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().vector_url

        super().__init__(url, auth=auth, retries=retries)
