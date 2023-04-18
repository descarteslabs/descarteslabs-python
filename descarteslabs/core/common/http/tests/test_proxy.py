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

import os
import unittest

from ..proxy import ProxyAuthentication


class TestProxyAuth(unittest.TestCase):
    def tearDown(self):
        ProxyAuthentication.unregister()
        ProxyAuthentication.clear_proxy()

        keys = ["HTTP_PROXY", "HTTPS_PROXY", "GRPC_PROXY"]
        for key in keys:
            os.environ.pop(key, None)

    def test_register_validates_type(self):
        with self.assertRaises(TypeError):
            ProxyAuthentication.register(dict)

    def test_requires_implementation(self):
        class MyProxyAuth(ProxyAuthentication):
            # unimplemented methods
            pass

        with self.assertRaises(TypeError):
            ProxyAuthentication.register(MyProxyAuth)

    def test_register(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                return {}

        my_instance = MyProxyAuth()
        ProxyAuthentication.register(MyProxyAuth)

        assert isinstance(ProxyAuthentication.get_registered_instance(), MyProxyAuth)
        assert ProxyAuthentication.get_registered_instance() != my_instance

        ProxyAuthentication.register(my_instance)
        assert ProxyAuthentication.get_registered_instance() == my_instance

        ProxyAuthentication.unregister()
        assert ProxyAuthentication.get_registered_instance() is None

    def test_proxies_from_env(self):
        os.environ["HTTP_PROXY"] = "http://some-proxy"
        assert ProxyAuthentication.get_proxies() == {
            "http": "http://some-proxy",
            "https": None,
            "grpc": "http://some-proxy",
        }

        os.environ["HTTPS_PROXY"] = "https://another-proxy"
        assert ProxyAuthentication.get_proxies() == {
            "http": "http://some-proxy",
            "https": "https://another-proxy",
            "grpc": "https://another-proxy",  # grpc defaults to https if not set
        }

        os.environ["GRPC_PROXY"] = "https://grpc-proxy"
        assert ProxyAuthentication.get_proxies() == {
            "http": "http://some-proxy",
            "https": "https://another-proxy",
            "grpc": "https://grpc-proxy",
        }

    def test_proxy_precedence(self):
        for protocol in ["http", "https", "grpc"]:
            env = f"{protocol.upper()}_PROXY"
            os.environ[env] = "set-by-env"

            ProxyAuthentication.set_proxy("set-by-user", protocol)

            proxies = ProxyAuthentication.get_proxies()
            other_protocol_proxies = [
                (k, v) for k, v in proxies.items() if k != protocol
            ]

            for k, v in other_protocol_proxies:
                if k == "grpc":
                    assert v == "set-by-env"
                else:
                    assert v is None

            ProxyAuthentication.clear_proxy()
            del os.environ[env]

    def test_set_proxy(self):
        assert ProxyAuthentication.get_proxies() == {
            "grpc": None,
            "http": None,
            "https": None,
        }

        for protocol in ProxyAuthentication.get_proxies().keys():
            proxy = f"http://proxy-{protocol}"
            ProxyAuthentication.set_proxy(proxy, protocol)
            assert ProxyAuthentication.get_proxy(protocol) == proxy

        ProxyAuthentication.clear_proxy()
        assert ProxyAuthentication.get_proxies() == {
            "grpc": None,
            "http": None,
            "https": None,
        }

        ProxyAuthentication.set_proxy("all-of-them")
        assert ProxyAuthentication.get_proxies() == {
            "grpc": "all-of-them",
            "http": "all-of-them",
            "https": "all-of-them",
        }

    def test_clear_proxy(self):
        assert ProxyAuthentication.get_proxies() != {}

        for protocol in ProxyAuthentication.get_proxies().keys():
            proxy = f"http://proxy-{protocol}"

            ProxyAuthentication.clear_proxy(protocol)
            ProxyAuthentication.set_proxy(proxy, protocol)
            assert ProxyAuthentication.get_proxy(protocol) == proxy

    def test_authorize(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                MyProxyAuth.proxy = proxy
                MyProxyAuth.protocol = protocol

                return {"some-header": "some-value"}

        ProxyAuthentication.register(MyProxyAuth)
        proxy_auth = ProxyAuthentication.get_registered_instance()
        assert isinstance(proxy_auth, ProxyAuthentication)

        headers = proxy_auth.get_verified_headers("http://some-proxy", "some-protocol")
        assert headers == {"some-header": "some-value"}
        assert MyProxyAuth.proxy == "http://some-proxy"
        assert MyProxyAuth.protocol == "some-protocol"

    def test_authorize_validation(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                MyProxyAuth.called = True
                return 10

        ProxyAuthentication.register(MyProxyAuth)
        proxy_auth = ProxyAuthentication.get_registered_instance()

        with self.assertRaisesRegex(TypeError, "return a dictionary"):
            proxy_auth.get_verified_headers("http://some proxy", "some protocol")

        assert MyProxyAuth.called
