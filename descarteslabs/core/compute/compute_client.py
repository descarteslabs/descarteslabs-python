from typing import Optional

from descarteslabs.config import get_settings

from ..auth import Auth
from ..client.services.service import Service
from ..common.http.service import DefaultClientMixin


class ComputeClient(Service, DefaultClientMixin):
    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().compute_url

        super().__init__(url, auth=auth, retries=retries)

    def paginate(self, url: str, params: Optional[dict] = None):
        if params is None:
            params = {}

        while True:
            response = self.session.get(url, params=params)
            response_json = response.json()
            data = response_json["data"]
            next_page = response_json["meta"]["page_cursor"]

            for item in data:
                yield item

            if not next_page:
                break

            params = {"page_cursor": next_page}

    def _remove_nulls(self, data: dict) -> dict:
        return {k: v for k, v in data.items() if v}
