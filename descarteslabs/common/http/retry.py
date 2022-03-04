import random

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RequestsWithRetry(object):
    """"""

    TIMEOUT = (1, 10)

    RETRY_CONFIG = Retry(
        total=3,
        read=2,
        backoff_factor=random.uniform(1, 3),
        allowed_methods=frozenset(
            ["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS", "HEAD"]
        ),
        status_forcelist=[500, 502, 503, 504],
    )

    ADAPTER = HTTPAdapter(max_retries=RETRY_CONFIG)

    def __init__(self, base_url="", headers=None):
        """
        Initialize a client with a base url and session headers.

        Parameters
        ----------
        base_url : str
        headers : dict
            Update the session headers applied to every request made with this client.
        """
        self.base_url = base_url
        self.headers = headers
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.mount(self.base_url, self.ADAPTER)
            self._session.headers.update(self.headers)

        return self._session

    def get(self, *args, **kwargs):
        return self.request("GET", *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.request("POST", *args, **kwargs)

    def put(self, *args, **kwargs):
        return self.request("PUT", *args, **kwargs)

    def patch(self, *args, **kwargs):
        return self.request("PATCH", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.request("delete", *args, **kwargs)

    def head(self, *args, **kwargs):
        return self.request("head", *args, **kwargs)

    def options(self, *args, **kwargs):
        return self.request("options", *args, **kwargs)

    def request(self, method, url, *args, **kwargs):
        return self.session.request(method, self.base_url + url, *args, **kwargs)
