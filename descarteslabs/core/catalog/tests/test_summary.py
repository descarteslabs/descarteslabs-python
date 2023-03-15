import json
import textwrap
from datetime import datetime
from urllib.parse import urlparse

import pytest
import responses

from .. import properties as p
from ..image import Image
from ..product import Product
from .base import ClientTestCase


class TestImageSummary(ClientTestCase):
    def setUp(self):
        super(TestImageSummary, self).setUp()
        self.search = Image.search(client=self.client)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    @responses.activate
    def test_image_summary(self):
        self.mock_response(
            responses.PUT,
            {
                "data": {
                    "attributes": {
                        "count": 1,
                        "bytes": 44306192,
                        "products": ["descarteslabs:fake-product"],
                    },
                    "type": "image_summary",
                    "id": "all",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        s = self.search.filter(p.product_id == "descarteslabs:fake-product")
        summary = s.summary()
        parsed_url = urlparse(responses.calls[0].request.url)
        assert parsed_url.path == "/catalog/v2/images/summary/all"
        params = self.get_request_body(0)

        assert json.loads(params["filter"]) == [
            {"name": "product_id", "val": "descarteslabs:fake-product", "op": "eq"}
        ]

        assert summary.products == ["descarteslabs:fake-product"]
        summary_repr = repr(summary)
        match_str = """\
            Summary for 1 images:
             - Total bytes: 44,306,192
             - Products: descarteslabs:fake-product"""

        assert summary_repr.strip("\n") == textwrap.dedent(match_str)

    INTERVAL_RESPONSE = {
        "meta": {"count": 1},
        "data": [
            {
                "attributes": {
                    "count": 1,
                    "interval_start": "2019-01-01T00:00:00Z",
                    "bytes": 44306192,
                },
                "type": "image_interval_summary",
                "id": "2019-01-01T00:00:00Z",
            }
        ],
        "jsonapi": {"version": "1.0"},
        "links": {
            "self": "https://www.example.com/catalog/v2/images/summary/acquired/year"
        },
    }

    @responses.activate
    def test_summary_interval(self):
        response = self.INTERVAL_RESPONSE
        response["links"] = dict(
            self="https://www.example.com/catalog/v2/images/summary/created/month"
        )
        self.mock_response(responses.PUT, response)

        results = self.search.summary_interval(
            aggregate_date_field="created",
            interval="month",
            start_datetime=datetime(2018, 1, 1),
            end_datetime="2019-01-01",
        )
        parsed_url = urlparse(responses.calls[0].request.url)
        assert parsed_url.path == "/catalog/v2/images/summary/created/month"

        request_params = self.get_request_body(0)
        assert request_params == {"_start": "2018-01-01T00:00:00", "_end": "2019-01-01"}

        assert len(results) == 1
        assert isinstance(results[0].interval_start, datetime)

    @responses.activate
    def test_summary_interval_defaults(self):
        self.mock_response(responses.PUT, self.INTERVAL_RESPONSE)
        results = self.search.summary_interval()
        parsed_url = urlparse(responses.calls[0].request.url)
        assert parsed_url.path == "/catalog/v2/images/summary/acquired/year"

        request_params = self.get_request_body(0)
        assert request_params == {}

        assert len(results) == 1
        assert isinstance(results[0].interval_start, datetime)

    @responses.activate
    def test_summary_interval_unbounded(self):
        self.mock_response(responses.PUT, self.INTERVAL_RESPONSE)
        self.search.summary_interval(start_datetime=0, end_datetime=0)
        parsed_url = urlparse(responses.calls[0].request.url)
        assert parsed_url.path == "/catalog/v2/images/summary/acquired/year"

        request_params = self.get_request_body(0)
        assert request_params == {"_start": "", "_end": ""}

    def test_invalid_summary(self):
        with pytest.raises(AttributeError):
            Product.search().summary()
