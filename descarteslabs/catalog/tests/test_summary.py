import pytest
import unittest
import responses
import re
import json
import textwrap
from datetime import datetime
from six.moves.urllib.parse import urlparse
from six import ensure_str

from descarteslabs.client.auth import Auth

from .. import properties as p
from ..catalog_base import CatalogClient
from ..image import Image
from ..product import Product

public_token = "header.e30.signature"


class TestImageSummary(unittest.TestCase):
    def setUp(self):
        self.url = "https://example.com/catalog/v2"
        self.client = CatalogClient(
            url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.search = Image.search(client=self.client)
        self.match_url = re.compile(self.url)

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
        params = json.loads(ensure_str(responses.calls[0].request.body))

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

    @responses.activate
    def test_summary_interval(self):
        self.mock_response(
            responses.PUT,
            {
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
                    "self": "https://www.example.com/catalog/v2/images/summary/created/month"
                },
            },
        )
        results = self.search.summary_interval(
            aggregate_date_field="created",
            interval="month",
            start_datetime=datetime(2018, 1, 1),
            end_datetime="2019-01-01",
        )
        parsed_url = urlparse(responses.calls[0].request.url)
        assert parsed_url.path == "/catalog/v2/images/summary/created/month"

        request_params = json.loads(ensure_str(responses.calls[0].request.body))
        assert request_params == {"_start": "2018-01-01T00:00:00", "_end": "2019-01-01"}

        assert len(results) == 1
        assert isinstance(results[0].interval_start, datetime)

    @responses.activate
    def test_summary_interval_defaults(self):
        self.mock_response(
            responses.PUT,
            {
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
            },
        )
        results = self.search.summary_interval()
        parsed_url = urlparse(responses.calls[0].request.url)
        assert parsed_url.path == "/catalog/v2/images/summary/acquired/year"

        assert len(results) == 1
        assert isinstance(results[0].interval_start, datetime)

    def test_invalid_summary(self):
        with pytest.raises(AttributeError):
            Product.search().summary()
