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

from descarteslabs.auth import Auth
from descarteslabs.config import get_settings

from ..client.services.service.service import HttpRequestMethod, JsonApiService
from ..common.http.service import DefaultClientMixin

HttpRequestMethod = HttpRequestMethod


class CatalogClient(JsonApiService, DefaultClientMixin):
    """
    The CatalogClient handles the HTTP communication with the Descartes Labs catalog.
    It is almost sufficient to use the default client that is automatically retrieved
    using `get_default_client`.  However, if you want to adjust e.g.  the retries, you
    can create your own.

    Parameters
    ----------
    url : str, optional
        The URL to use when connecting to the Descartes Labs catalog.  Only change
        this if you are being asked to use a non-default Descartes Labs catalog.  If
        not set, then ``descarteslabs.config.get_settings().CATALOG_V2_URL`` will be used.
    auth : Auth, optional
        The authentication object used when connecting to the Descartes Labs catalog.
        This is typically the default :class:`~descarteslabs.auth.Auth` object that uses
        the cached authentication
        token retrieved with the shell command "$ descarteslabs auth login".
    retries : int, optional
        The number of retries when there is a problem with the connection.  Set this to
        zero to disable retries.  The default is 3 retries.
    """

    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().catalog_v2_url

        super(CatalogClient, self).__init__(
            url, auth=auth, retries=retries, rewrite_errors=True
        )
