import unittest

import responses

from ..retry import RequestsWithRetry


class TestRequestsWithRetry(unittest.TestCase):
    @responses.activate
    def test_headers_set_on_session(self):
        token = "token"
        client = RequestsWithRetry(headers=dict(authorization=token))
        assert client.session.headers["authorization"] == token

        def request_callback(request):
            if request.headers["authorization"] == token:
                return 200, {}, ""
            return 401, {}, ""

        for method in ["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS", "HEAD"]:
            responses.add_callback(
                method, "https://platform.descarteslabs.com", callback=request_callback
            )
            assert (
                getattr(client, method.lower())(
                    "https://platform.descarteslabs.com"
                ).status_code
                == 200
            )


if __name__ == "__main__":
    unittest.main()
