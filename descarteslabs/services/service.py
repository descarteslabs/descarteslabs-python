"""
"""
import random
import os

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import descarteslabs.cli_auth


class Service:

    def __init__(self, url, token):
        self.auth = descarteslabs.cli_auth.Auth()
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
                        backoff_factor=random.uniform(1, 10),
                        status_forcelist=[500, 502, 503, 504])

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
