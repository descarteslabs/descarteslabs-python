import random
import os

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import descarteslabs.auth


class Service:
    TIMEOUT = 30

    def __init__(self, url, token):
        self.auth = descarteslabs.auth.Auth()
        self.url = url
        if token:
            self.auth._token = token

    @property
    def token(self):
        return self.auth.token

    @property
    def session(self):

        s = requests.Session()

        retries = Retry(total=5,
                        read=2,
                        backoff_factor=random.uniform(1, 3),
                        method_whitelist=frozenset([
                            'HEAD', 'TRACE', 'GET', 'POST',
                            'PUT', 'OPTIONS', 'DELETE'
                        ]),
                        status_forcelist=[429, 500, 502, 503, 504])

        s.mount('http://', HTTPAdapter(max_retries=retries))

        s.headers.update({"Authorization": self.token})
        s.headers.update({"content-type": "application/json"})

        here = os.path.dirname(__file__)

        try:
            file = os.path.join(here, 'gd_bundle-g2-g1.crt')
            with open(file):
                s.verify = file
        except:
            s.verify = False

        return s
