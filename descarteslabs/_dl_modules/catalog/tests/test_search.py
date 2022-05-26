import responses
import json
import shapely.geometry

from .base import ClientTestCase
from .. import properties as p
from ..search import Search
from ..image import Image
from ..attributes import DocumentState
from ..product import Product


class TestSearch(ClientTestCase):
    def setUp(self):
        super(TestSearch, self).setUp()
        self.search = Search(Product, client=self.client)

    def sort_filters(self, filters):
        """
        Sort all lists in filter definitions in a more or less stable way
        in place for comparison.
        """
        if type(filters) == list:
            filters.sort(
                key=lambda i: (
                    len(i.get("and", [])),
                    len(i.get("or", [])),
                    i.get("name", ""),
                    i.get("op", ""),
                    i.get("val", ""),
                )
            )
            for filter in filters:
                self.sort_filters(filter)
        elif "and" in filters:
            self.sort_filters(filters["and"])
        elif "or" in filters:
            self.sort_filters(filters["or"])
        return filters

    @responses.activate
    def test_search(self):
        assert self.search._to_request() == ("/products", {})
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "My Product",
                            "readers": [],
                            "modified": "2019-06-12T20:31:48.542725Z",
                            "created": "2019-06-12T20:31:48.542725Z",
                            "start_datetime": None,
                            "writers": [],
                            "end_datetime": None,
                            "description": "This is a test product",
                        },
                        "type": "product",
                        "id": "descarteslabs:my-product",
                    }
                ],
                "jsonapi": {"version": "1.0"},
                "links": {
                    "self": "https://example.com/catalog/v2/products",
                    "next": "https://example.com/catalog/v2/products?continuation=.xxx",
                },
            },
        )

        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )
        results = list(self.search)
        assert len(results) == 1
        assert type(results[0]) == Product
        # followed continuation token
        assert responses.calls[0].request.url == self.url + "/products"
        assert (
            responses.calls[1].request.url == self.url + "/products?continuation=.xxx"
        )

    @responses.activate
    def test_count(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )

        count = self.search.count()
        assert self.get_request_body(0) == {"limit": 0}
        assert count == 1

    @responses.activate
    def test_count_limit(self):
        s = self.search.limit(10)
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )
        count = s.count()
        # limit has no impact for count request
        assert self.get_request_body(0) == {"limit": 0}
        assert count == 1

    @responses.activate
    def test_filter_single(self):
        s = self.search.filter(p.revisit_period_minutes_min == 60)
        assert s._serialize_filters() == [
            {"op": "eq", "name": "revisit_period_minutes_min", "val": 60}
        ]

        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "My Product",
                            "readers": [],
                            "modified": "2019-06-12T20:31:48.542725Z",
                            "created": "2019-06-12T20:31:48.542725Z",
                            "start_datetime": None,
                            "writers": [],
                            "end_datetime": None,
                            "description": "This is a test product",
                            "revisit_period_minutes_min": 60,
                        },
                        "type": "product",
                        "id": "descarteslabs:my-product",
                    }
                ],
                "jsonapi": {"version": "1.0"},
                "links": {
                    "self": "https://example.com/catalog/v2/products",
                    "next": "https://example.com/catalog/v2/products?continuation=.xxx",
                },
            },
        )
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )

        results = list(s)
        assert len(results) == 1
        product = results[0]
        assert product.revisit_period_minutes_min == 60
        assert product._saved
        assert product.state == DocumentState.SAVED

    def test_sort(self):
        s = self.search.sort("start_datetime").sort("created", ascending=False)
        assert s._to_request() == ("/products", {"sort": "-created"})

    def test_filter_nested(self):
        s = self.search.filter(
            (
                (p.tags == "test")
                & (
                    (p.start_datetime == "2016-01-02")
                    | (p.start_datetime == "2016-01-01")
                )
            )
            & (1000 >= p.revisit_period_minutes_max > 100)
        )
        filters = s._serialize_filters()
        assert self.sort_filters(filters) == self.sort_filters(
            [
                {"name": "tags", "val": "test", "op": "eq"},
                {
                    "or": [
                        {"name": "start_datetime", "val": "2016-01-02", "op": "eq"},
                        {"name": "start_datetime", "val": "2016-01-01", "op": "eq"},
                    ]
                },
                {
                    "and": [
                        {"name": "revisit_period_minutes_max", "val": 100, "op": "gt"},
                        {
                            "name": "revisit_period_minutes_max",
                            "val": 1000,
                            "op": "lte",
                        },
                    ]
                },
            ]
        )

    def test_filter_range_only(self):
        s = self.search.filter(p.revisit_period_minutes_max > 100)
        filters = s._serialize_filters()
        assert filters == [
            {"name": "revisit_period_minutes_max", "val": 100, "op": "gt"}
        ]

    def test_filter_multirange_nested_or(self):
        s = self.search.filter(
            (1000 >= p.revisit_period_minutes_max > 100)
            | (p.revisit_period_minutes_max > 2000)
        )
        filters = s._serialize_filters()
        assert self.sort_filters(filters) == self.sort_filters(
            [
                {
                    "or": [
                        {
                            "and": [
                                {
                                    "name": "revisit_period_minutes_max",
                                    "val": 100,
                                    "op": "gt",
                                },
                                {
                                    "name": "revisit_period_minutes_max",
                                    "val": 1000,
                                    "op": "lte",
                                },
                            ]
                        },
                        {"name": "revisit_period_minutes_max", "val": 2000, "op": "gt"},
                    ]
                }
            ]
        )

    def test_filter_contains(self):
        s = self.search.filter(p.revisit_period_minutes_min.in_([60, 120]))
        filters = s._serialize_filters()
        assert filters == [
            {
                "or": [
                    {"name": "revisit_period_minutes_min", "val": 60, "op": "eq"},
                    {"name": "revisit_period_minutes_min", "val": 120, "op": "eq"},
                ]
            }
        ]

    def test_filter_geometry(self):
        geometry = {
            "type": "Polygon",
            "coordinates": (
                (
                    (-9.000262842437783, 46.9537091787344),
                    (-8.325270159894608, 46.95172107428039),
                    (-8.336543403548475, 46.925857032669434),
                    (-8.39987774007129, 46.7807657614384),
                    (-8.463235968271405, 46.63558741606639),
                    (-8.75144712554016, 45.96528086358922),
                    (-9.0002581299532, 45.9655511480415),
                    (-9.000262842437783, 46.9537091787344),
                ),
            ),
        }

        s = Search(Image, client=self.client).filter(
            p.geometry == shapely.geometry.shape(geometry)
        )
        filters = s._serialize_filters()
        assert filters[0]["val"] == geometry

    def test_filter_object(self):
        my_product = Product(id="my_product")

        s = Search(Image, client=self.client)
        s = s.filter(p.product == my_product)
        filters = s._serialize_filters()
        assert filters[0]["name"] == "product_id"
        assert filters[0]["val"] == my_product.id

        s = Search(Image, client=self.client)
        s = s.filter(p.product != my_product)
        filters = s._serialize_filters()
        assert filters[0]["name"] == "product_id"
        assert filters[0]["val"] == my_product.id

        # Not supported for <, <=, >, >=

    @responses.activate
    def test_filter_resolution(self):
        s = self.search.filter(p.resolution_min == 60)
        assert s._serialize_filters() == [
            {"op": "eq", "name": "resolution_min", "val": 60}
        ]

    @responses.activate
    def test_limit(self):
        s = self.search.limit(2)
        assert s._to_request() == ("/products", {"limit": 2})
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 2},
                "data": [
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "P1",
                            "readers": [],
                            "modified": "2019-06-12T20:31:48.542725Z",
                            "created": "2019-06-12T20:31:48.542725Z",
                            "description": "This is a test product",
                        },
                        "type": "product",
                        "id": "descarteslabs:p1",
                    },
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "P2",
                            "readers": [],
                            "modified": "2019-06-12T20:31:48.542725Z",
                            "created": "2019-06-12T20:31:48.542725Z",
                            "description": "This is a test product",
                        },
                        "type": "product",
                        "id": "descarteslabs:p2",
                    },
                ],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )
        results = list(s)
        assert len(results) == 2
        # does not follow continuation token after limit is reached
        assert len(responses.calls) == 1

    def test_search_find_text(self):
        s = self.search.find_text("test")
        assert s._to_request() == ("/products", {"text": "test"})

        s = (
            self.search.limit(10)
            .filter(p.tags == "drone")
            .find_text("test")
            .sort("start_datetime")
        )

        _, request_params = s._to_request()
        assert json.loads(request_params["filter"]) == [
            {"name": "tags", "val": "drone", "op": "eq"}
        ]
        assert request_params["limit"] == 10
        assert request_params["sort"] == "start_datetime"
        assert request_params["text"] == "test"

    def test_default_includes(self):
        s = Search(Image, client=self.client)
        assert s._to_request() == ("/images", {"include": "product"})
