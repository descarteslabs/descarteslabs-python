# Copyright 2018-2020 Descartes Labs.
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

import mock
import descarteslabs
from descarteslabs.client.exceptions import (
    ProxyAuthenticationRequiredError,
    BadRequestError,
)
from descarteslabs.client.services.service import (
    JsonApiService,
    JsonApiSession,
    Service,
    Session,
    ThirdPartyService,
)
from descarteslabs.client.services.service.service import (
    HttpHeaderKeys,
    HttpHeaderValues,
    HttpRequestMethod,
    HttpStatusCode,
    WrappedSession,
    requests,
)
from descarteslabs.client.version import __version__
from descarteslabs.common.http.authorization import add_bearer

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
        request.return_value.status_code = HttpStatusCode.Ok

        args = "POST", FAKE_URL
        kwargs = dict(headers={"X-Request-Group": "f00"})

        session = WrappedSession("")
        session.request(*args, **kwargs)
        request.assert_called_once_with(*args, **kwargs)  # we do nothing here

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_no_conflict(self, request):
        request.return_value.status_code = HttpStatusCode.Ok

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
        request.return_value.status_code = HttpStatusCode.Ok

        class MySession(Session):
            pass

        service = Service(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        request.assert_called()

    @mock.patch.object(requests.Session, "request")
    def test_bad_json_session(self, request):
        request.return_value.status_code = HttpStatusCode.Ok

        class MySession(Session):
            pass

        with self.assertRaises(TypeError):
            JsonApiService(
                "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
            )

    @mock.patch.object(requests.Session, "request")
    def test_good_json_session(self, request):
        request.return_value.status_code = HttpStatusCode.Ok

        class MySession(JsonApiSession):
            pass

        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        request.assert_called()

    @mock.patch.object(requests.Session, "request")
    def test_proxy_called(self, request):
        request.return_value.status_code = HttpStatusCode.ProxyAuthenticationRequired

        class MySession(Session):
            handle_proxy_authentication_called = 0
            handled = True

            def handle_proxy_authentication(self, method, url, **kwargs):
                MySession.handle_proxy_authentication_called += 1
                assert method == HttpRequestMethod.GET
                assert url == "bar"
                return MySession.handled

        service = Service(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 1

        MySession.handled = False
        with self.assertRaises(ProxyAuthenticationRequiredError):
            service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 2

    @mock.patch.object(requests.Session, "request")
    def test_proxy_called_jsonapi(self, request):
        request.return_value.status_code = HttpStatusCode.ProxyAuthenticationRequired

        class MySession(JsonApiSession):
            handle_proxy_authentication_called = 0
            handled = True

            def handle_proxy_authentication(self, method, url, **kwargs):
                MySession.handle_proxy_authentication_called += 1
                assert method == HttpRequestMethod.GET
                assert url == "bar"
                return MySession.handled

        service = JsonApiService(
            "foo", auth=mock.MagicMock(token=FAKE_TOKEN), session_class=MySession
        )
        service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 1

        MySession.handled = False
        with self.assertRaises(ProxyAuthenticationRequiredError):
            service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 2

    @mock.patch.object(requests.Session, "request")
    def test_proxy_called_thirdpary(self, request):
        request.return_value.status_code = HttpStatusCode.ProxyAuthenticationRequired

        class MySession(Session):
            handle_proxy_authentication_called = 0
            handled = True

            def handle_proxy_authentication(self, method, url, **kwargs):
                MySession.handle_proxy_authentication_called += 1
                assert method == HttpRequestMethod.GET
                assert url == "bar"
                return MySession.handled

        service = ThirdPartyService(session_class=MySession)
        service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 1

        MySession.handled = False
        with self.assertRaises(ProxyAuthenticationRequiredError):
            service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 2


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

        request.return_value.status_code = HttpStatusCode.BadRequest
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

        request.return_value.status_code = HttpStatusCode.BadRequest
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

        request.return_value.status_code = HttpStatusCode.BadRequest
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

        request.return_value.status_code = HttpStatusCode.BadRequest
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

        request.return_value.status_code = HttpStatusCode.BadRequest
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

        request.return_value.status_code = HttpStatusCode.BadRequest
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


class TestDefaultProxyClass(unittest.TestCase):
    @mock.patch.object(requests.Session, "request")
    def test_session_default_proxy(self, request):
        request.return_value.status_code = HttpStatusCode.ProxyAuthenticationRequired

        class MySession(Session):
            handle_proxy_authentication_called = 0
            handled = True

            def handle_proxy_authentication(self, method, url, **kwargs):
                MySession.handle_proxy_authentication_called += 1
                assert method == HttpRequestMethod.GET
                assert url == "bar"
                return MySession.handled

        Service.set_default_session_class(MySession)
        service = Service("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 1

        MySession.handled = False
        with self.assertRaises(ProxyAuthenticationRequiredError):
            service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 2

        MySession.handled = True
        ThirdPartyService.set_default_session_class(MySession)
        service = ThirdPartyService()
        service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 3

        MySession.handled = False
        with self.assertRaises(ProxyAuthenticationRequiredError):
            service.session.get("bar")

        assert MySession.handle_proxy_authentication_called == 4


class TestWarningsClass(unittest.TestCase):
    @mock.patch.object(descarteslabs.client.services.service.service, "warn")
    @mock.patch.object(requests.Session, "request")
    def test_session_deprecation_warning(self, request, warn):
        message = "Warning"
        cls = DeprecationWarning

        class result:
            status_code = HttpStatusCode.Ok

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

    @mock.patch.object(descarteslabs.client.services.service.service, "warn")
    @mock.patch.object(requests.Session, "request")
    def test_session_my_warning(self, request, warn):
        message = "Warning"
        category = "MyCategory"

        class result:
            status_code = HttpStatusCode.Ok

            def json(self):
                return {
                    "meta": {"warnings": [{"message": message, "category": category}]}
                }

        request.side_effect = lambda *args, **kw: result()
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("bar")
        warn.assert_called_once_with("{}: {}".format(category, message), UserWarning)

    @mock.patch.object(descarteslabs.client.services.service.service, "warn")
    @mock.patch.object(requests.Session, "request")
    def test_session_warning(self, request, warn):
        message = "Warning"

        class result:
            status_code = HttpStatusCode.Ok

            def json(self):
                return {"meta": {"warnings": [{"message": message}]}}

        request.side_effect = lambda *args, **kw: result()
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        service.session.get("bar")
        warn.assert_called_once_with(message, UserWarning)


class TestInitialize(unittest.TestCase):
    @mock.patch.object(requests.Session, "request")
    def test_initialize_session(self, request):
        request.return_value.status_code = HttpStatusCode.Ok

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
        request.return_value.status_code = HttpStatusCode.Ok

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
        request.return_value.status_code = HttpStatusCode.Ok

        class MySession(Session):
            initialize_called = 0

            def initialize(self):
                MySession.initialize_called += 1

        service = ThirdPartyService(session_class=MySession)
        service.session.get("bar")

        assert MySession.initialize_called == 1
