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

import json
from datetime import datetime, timezone
from typing import Iterator, Optional

from descarteslabs.auth import Auth
from descarteslabs.config import get_settings

from ..catalog import CatalogClient
from ..client.services.service import ApiService
from ..common.http.service import DefaultClientMixin


class ComputeClient(ApiService, DefaultClientMixin):
    _namespace_cache = dict()

    def __init__(self, url=None, auth=None, catalog_client=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if catalog_client is None:
            catalog_client = CatalogClient(auth=auth)

        if url is None:
            url = get_settings().compute_url

        self.catalog_client = catalog_client
        super().__init__(url, auth=auth, retries=retries)

    def iter_log_lines(self, url: str, timestamps: bool = True) -> Iterator[str]:
        response = self.session.get(url, stream=True)
        lines = response.iter_lines()

        for line in lines:
            structured_log = json.loads(line)
            timestamp, log = structured_log["date"], structured_log["log"]
            log_date = (
                datetime.fromisoformat(timestamp[:-1] + "+00:00")
                .replace(tzinfo=timezone.utc)
                .astimezone()  # Convert to users timezone
            )

            if timestamps:
                log = f"{log_date} {log}"

            yield log

    def check_credentials(self):
        """Determine if valid credentials are already set for the user."""
        _ = self.session.get("/credentials")

    def set_credentials(self):
        if self.auth.client_id and self.auth.client_secret:
            self.session.post(
                "/credentials",
                json={
                    "client_id": self.auth.client_id,
                    "client_secret": self.auth.client_secret,
                },
            )
        else:
            # We only have a JWT and no client id/secret, so validate
            # that there are already valid credentials set for the user.
            # This situation will generally only arise when used from
            # some backend process.
            self.check_credentials()

    def get_namespace(self, function_id: str) -> Optional[str]:
        if function_id in self._namespace_cache:
            return self._namespace_cache[function_id]

        return None

    def set_namespace(self, function_id: str, namespace: str):
        if not (function_id or namespace):
            return

        self._namespace_cache[function_id] = namespace
