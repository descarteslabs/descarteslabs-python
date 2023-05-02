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
from typing import Iterator

from descarteslabs.auth import Auth
from descarteslabs.config import get_settings

from ..client.services.service import ApiService
from ..common.http.service import DefaultClientMixin


class ComputeClient(ApiService, DefaultClientMixin):
    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().compute_url

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

    def set_credentials(self):
        self.session.post(
            "/credentials",
            json={
                "client_id": self.auth.client_id,
                "client_secret": self.auth.client_secret,
            },
        )
