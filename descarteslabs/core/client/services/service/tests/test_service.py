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

import pickle
import unittest
from http import HTTPStatus
from io import BytesIO

from unittest import mock
import requests
import responses
import urllib3
from descarteslabs.exceptions import BadRequestError, ProxyAuthenticationRequiredError

from .....common.http.authorization import add_bearer
from .....common.http import ProxyAuthentication
from ....version import __version__
from .. import (
    JsonApiService,
    JsonApiSession,
    Service,
    Session,
    ThirdPartyService,
    service,
)
from ..service import HttpHeaderKeys, HttpHeaderValues, WrappedSession

FAKE_URL = "http://localhost"
FAKE_TOKEN = "foo.bar.sig"


class TestService(unittest.TestCase):
    def test_session_token(self):
        service = Service("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        assert service.session.headers.get("Authorization") == add_bearer(FAKE_TOKEN)

    def test_client_session_header(self):
        service = Service("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        assert "X-Client-Session" in service.session.headers
        assert (
            service.session.headers[HttpHeaderKeys.ContentType]
            == HttpHeaderValues.ApplicationJson
        )
        assert service.session.headers[HttpHeaderKeys.UserAgent] == "{}/{}".format(
            HttpHeaderValues.DlPython, __version__
        )


class TestJsonApiService(unittest.TestCase):
    def test_session_token(self):
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        assert service.session.headers.get("Authorization") == add_bearer(FAKE_TOKEN)

    def test_client_session_header(self):
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        assert "X-Client-Session" in service.session.headers
        assert (
            service.session.headers[HttpHeaderKeys.ContentType]
            == HttpHeaderValues.ApplicationVndApiJson
        )
        assert service.session.headers[HttpHeaderKeys.UserAgent] == "{}/{}".format(
            HttpHeaderValues.DlPython, __version__
        )


class TestThirdParyService(unittest.TestCase):
    def test_client_session_header(self):
        service = ThirdPartyService()
        assert "User-Agent" in service.session.headers


class TestWrappedSession(unittest.TestCase):
    def test_pickling(self):
        session = WrappedSession(FAKE_URL, timeout=10)
        assert 10 == session.timeout
        unpickled = pickle.loads(pickle.dumps(session))
        assert 10 == unpickled.timeout

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_none(self, request):
        request.return_value.status_code = 200

        session = WrappedSession("")
        session.request("POST", FAKE_URL)

        request.assert_called_once()
        assert "X-Request-Group" in request.call_args[1]["headers"]

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_conflict(self, request):
        request.return_value.status_code = HTTPStatus.OK

        args = "POST", FAKE_URL
        kwargs = dict(headers={"X-Request-Group": "f00"})

        session = WrappedSession("")
        session.request(*args, **kwargs)
        request.assert_called_once_with(*args, **kwargs)  # we do nothing here

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_no_conflict(self, request):
        request.return_value.status_code = HTTPStatus.OK

        session = WrappedSession("")
        session.request("POST", FAKE_URL, headers={"foo": "bar"})

        request.assert_called_once()
        assert "X-Request-Group" in request.call_args[1]["headers"]


class TestSessionClass(unittest.TestCase):
    def test_bad_session(self):
        class MySession:
            pass

        with self.assertRaises(TypeError):
            Service(
                "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
            )

    @mock.patch.object(requests.Session, "request")
    def test_good_session(self, request):
        request.return_value.status_code = HTTPStatus.OK

        class MySession(Session):
            pass

        service = Service(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        request.assert_called()

    @mock.patch.object(requests.Session, "request")
    def test_bad_json_session(self, request):
        request.return_value.status_code = HTTPStatus.OK

        class MySession(Session):
            pass

        with self.assertRaises(TypeError):
            JsonApiService(
                "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
            )

    @mock.patch.object(requests.Session, "request")
    def test_good_json_session(self, request):
        request.return_value.status_code = HTTPStatus.OK

        class MySession(JsonApiSession):
            pass

        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        request.assert_called()


class TestJsonApiSession(unittest.TestCase):
    # A JSONAPI error can contain, amongst others, the following fields:
    #     status, title, detail, source
    # The source field can contain:
    #     pointer, parameter
    # When rewriting the error, it looks like
    #     [title or status: ][description: ][source or parameter][ (id)][
    #         link]

    @mock.patch.object(requests.Session, "request")
    def test_jsonapi_error(self, request):
        error_title = "Title"
        error_status = "Status"  # Should be ignored

        request.return_value.status_code = HTTPStatus.BAD_REQUEST
        request.return_value.text = (
            '{{"errors": [{{"title": "{}", "status": "{}"}}]}}'
        ).format(error_title, error_status)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == ("\n    {}".format(error_title),)

    @mock.patch.object(requests.Session, "request")
    def test_jsonapi_error_with_detail(self, request):
        error_title = "Title"
        error_detail = "Description"

        request.return_value.status_code = HTTPStatus.BAD_REQUEST
        request.return_value.text = (
            '{{"errors": [{{"title": "{}", "detail": "{}"}}]}}'
        ).format(error_title, error_detail)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == ("\n    {}: {}".format(error_title, error_detail),)

    @mock.patch.object(requests.Session, "request")
    def test_jsonapi_error_no_title(self, request):
        error_status = "Status"  # Should be used instead of the title
        error_detail = "Description"

        request.return_value.status_code = HTTPStatus.BAD_REQUEST
        request.return_value.text = (
            '{{"errors": [{{"status": "{}", "detail": "{}"}}]}}'
        ).format(error_status, error_detail)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == ("\n    {}: {}".format(error_status, error_detail),)

    @mock.patch.object(requests.Session, "request")
    def test_jsonapi_error_with_source(self, request):
        error_title = "Title"
        error_detail = "Detail"
        error_field = "Field"

        request.return_value.status_code = HTTPStatus.BAD_REQUEST
        request.return_value.text = (
            '{{"errors": [{{"title": "{}", "detail": "{}", "source": '
            '{{"pointer": "/path/to/{}"}}}}]}}'
        ).format(error_title, error_detail, error_field)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == (
                "\n    {}: {}: {}".format(error_title, error_detail, error_field),
            )

    @mock.patch.object(requests.Session, "request")
    def test_jsonapi_error_with_id(self, request):
        error_title = "Title"
        error_detail = "Detail"
        error_id = "123"

        request.return_value.status_code = HTTPStatus.BAD_REQUEST
        request.return_value.text = (
            '{{"errors": [{{"title": "{}", "detail": "{}", "id": {}}}]}}'
        ).format(error_title, error_detail, error_id)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == (
                "\n    {}: {} ({})".format(error_title, error_detail, error_id),
            )

    @mock.patch.object(requests.Session, "request")
    def test_jsonapi_error_with_link(self, request):
        error_title = "Title"
        error_detail = "Detail"
        error_href = "Href"

        request.return_value.status_code = HTTPStatus.BAD_REQUEST
        request.return_value.text = (
            '{{"errors": [{{"title": "{}", "detail": "{}", "links": '
            '{{"about": "{}"}}}}]}}'
        ).format(error_title, error_detail, error_href)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == (
                "\n    {}: {}\n        {}".format(
                    error_title, error_detail, error_href
                ),
            )

        request.return_value.text = (
            '{{"errors": [{{"title": "{}", "detail": "{}", "links": '
            '{{"about": {{"href": "{}"}}}}}}]}}'
        ).format(error_title, error_detail, error_href)
        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), rewrite_errors=True
        )

        try:
            service.session.get("bar")
        except BadRequestError as e:
            assert e.args == (
                "\n    {}: {}\n        {}".format(
                    error_title, error_detail, error_href
                ),
            )


class TestWarningsClass(unittest.TestCase):
    @mock.patch.object(service, "warn")
    @mock.patch.object(requests.Session, "request")
    def test_session_deprecation_warning(self, request, warn):
        message = "Warning"
        cls = FutureWarning

        class result:
            status_code = HTTPStatus.OK

            def json(self):
                return {
                    "meta": {
                        "warnings": [{"message": message, "category": cls.__name__}]
                    }
                }

        request.side_effect = lambda *args, **kw: result()
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("bar")
        warn.assert_called_once_with(message, cls)

    @mock.patch.object(service, "warn")
    @mock.patch.object(requests.Session, "request")
    def test_session_my_warning(self, request, warn):
        message = "Warning"
        category = "MyCategory"

        class result:
            status_code = HTTPStatus.OK

            def json(self):
                return {
                    "meta": {"warnings": [{"message": message, "category": category}]}
                }

        request.side_effect = lambda *args, **kw: result()
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("bar")
        warn.assert_called_once_with("{}: {}".format(category, message), UserWarning)

    @mock.patch.object(service, "warn")
    @mock.patch.object(requests.Session, "request")
    def test_session_warning(self, request, warn):
        message = "Warning"

        class result:
            status_code = HTTPStatus.OK

            def json(self):
                return {"meta": {"warnings": [{"message": message}]}}

        request.side_effect = lambda *args, **kw: result()
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("bar")
        warn.assert_called_once_with(message, UserWarning)


class TestInitialize(unittest.TestCase):
    @mock.patch.object(requests.Session, "request")
    def test_initialize_session(self, request):
        request.return_value.status_code = HTTPStatus.OK

        class MySession(Session):
            initialize_called = 0

            def initialize(self):
                MySession.initialize_called += 1

        service = Service(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        assert MySession.initialize_called == 1

    @mock.patch.object(requests.Session, "request")
    def test_initialize_json_api_session(self, request):
        request.return_value.status_code = HTTPStatus.OK

        class MySession(JsonApiSession):
            initialize_called = 0

            def initialize(self):
                MySession.initialize_called += 1

        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        assert MySession.initialize_called == 1

    @mock.patch.object(requests.Session, "request")
    def test_initialize_third_party_session(self, request):
        request.return_value.status_code = HTTPStatus.OK

        class MySession(Session):
            initialize_called = 0

            def initialize(self):
                MySession.initialize_called += 1

        service = ThirdPartyService(session_class=MySession)
        service.session.get("bar")

        assert MySession.initialize_called == 1


class TestProxyAuthHTTPS(unittest.TestCase):
    url = "https://fake-service"
    protocol = ProxyAuthentication.Protocol.HTTPS

    def tearDown(self):
        ProxyAuthentication.unregister()
        ProxyAuthentication.clear_proxy()

    @responses.activate
    def test_requires_proxy_auth(self):
        responses.add("GET", self.url + "/bar", status=407)

        service = Service(self.url, auth=mock.MagicMock(token=FAKE_TOKEN))

        with self.assertRaises(ProxyAuthenticationRequiredError):
            service.session.get("/bar")

    # responses hijacks the connection pool and bypasses our HTTPAdapter
    # unfortunately we have to mock the pool manager here instead.
    @mock.patch(
        "urllib3.poolmanager.PoolManager._new_pool",
    )
    def test_no_proxy_headers_if_proxy_not_set(self, mock_conn):
        mock_conn.return_value.urlopen.side_effect = [
            urllib3.response.HTTPResponse(
                status=200,
                reason=None,
                body=BytesIO(),
                headers=[],
                preload_content=False,
            ),
        ]

        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                MyProxyAuth.proxy = proxy
                MyProxyAuth.protocol = protocol

                return {"header-1": "uh oh"}

        ProxyAuthentication.register(MyProxyAuth)

        service = Service(self.url, auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("/bar")

        assert not hasattr(MyProxyAuth, "proxy")
        assert not hasattr(MyProxyAuth, "protocol")
        assert mock_conn.called

        _, kwargs = mock_conn.call_args

        assert "_proxy" not in kwargs["request_context"]
        assert "_proxy_headers" not in kwargs["request_context"]

    @mock.patch(
        "urllib3.poolmanager.PoolManager._new_pool",
    )
    def test_proxy_authentication(self, mock_conn):
        mock_conn.return_value.urlopen.side_effect = [
            urllib3.response.HTTPResponse(
                status=200,
                reason=None,
                body=BytesIO(),
                headers=[],
                preload_content=False,
            ),
        ]

        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                MyProxyAuth.proxy = proxy
                MyProxyAuth.protocol = protocol

                return {
                    "Proxy-Authorization": "proxy-auth-value",
                    "X-Test-Header": "another test header",
                }

        ProxyAuthentication.register(MyProxyAuth)
        ProxyAuthentication.set_proxy("http://some-proxy.test")

        service = Service(self.url, auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("/bar")

        assert MyProxyAuth.proxy == "http://some-proxy.test"
        assert MyProxyAuth.protocol == self.protocol
        assert mock_conn.called

        args, kwargs = mock_conn.call_args
        assert str(kwargs["request_context"]["_proxy"]) == "http://some-proxy.test:80"
        assert kwargs["request_context"]["_proxy_headers"] == {
            "Proxy-Authorization": "proxy-auth-value",
            "X-Test-Header": "another test header",
        }
        assert kwargs["request_context"]["scheme"] == self.protocol
        assert (
            kwargs["request_context"]["port"] == 80
            if self.protocol == ProxyAuthentication.Protocol.HTTP
            else 443
        )

        # The request is tunneling it should be directed at the real service instead of
        # the proxy
        if self.protocol == ProxyAuthentication.Protocol.HTTPS:
            assert kwargs["request_context"]["host"] == "fake-service"
        else:
            assert kwargs["request_context"]["host"] == "some-proxy.test"

    def test_validates_authorize(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                MyProxyAuth.called = True
                return 10

        ProxyAuthentication.register(MyProxyAuth)
        ProxyAuthentication.set_proxy("http://some-proxy.test:8888")

        with self.assertRaisesRegex(TypeError, "must return a dictionary"):
            service = Service(self.url, auth=mock.MagicMock(token=FAKE_TOKEN))
            service.session.get("/bar")
            assert MyProxyAuth.called


class TestProxyAuthHTTP(TestProxyAuthHTTPS):
    url = "http://fake-service"
    protocol = ProxyAuthentication.Protocol.HTTP

    @responses.activate
    def test_proxy_auth_required_headers(self):
        responses.add(
            "GET",
            self.url + "/bar",
            status=407,
            headers={
                "Proxy-Authenticate": "Basic",
            },
        )

        service = Service(self.url, auth=mock.MagicMock(token=FAKE_TOKEN))

        with self.assertRaises(ProxyAuthenticationRequiredError) as ctx:
            service.session.get("/bar")

        assert ctx.exception.status == 407
        assert ctx.exception.proxy_authenticate == "Basic"
