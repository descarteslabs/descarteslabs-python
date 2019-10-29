import copy
import json
import warnings

from .catalog_base import CatalogClient
from descarteslabs.common.property_filtering.filtering import AndExpression
from descarteslabs.common.property_filtering.filtering import Expression  # noqa: F401

from .attributes import parse_iso_datetime, serialize_datetime


class Search(object):
    """A search request that iterates over its search results.

    You can narrow your search by using the following methods on the search object:

    * :py:meth:`limit`
    * :py:meth:`filter`
    * :py:meth:`find_text`

    Each method returns a narrowed-down search object.  You obtain a search object
    using the :py:meth:`~descarteslabs.catalog.CatalogObject.search` method
    on a :py:class:`~descarteslabs.catalog.CatalogObject` such as
    :py:class:`~descarteslabs.catalog.Product`, :py:class:`~descarteslabs.catalog.Band`
    or :py:class:`~descarteslabs.catalog.Image`.

    You must use the `Search` object as an ``iterator`` to get the results.
    This will execute the search query and return a generator for iterating through
    the returned results.  This might raise a `BadRequestError` if any of the query
    parameters or filters are invalid.

    Example
    -------
    >>> search = Search(Product).filter(p.start_datetime >= "2012-01-01")
    >>> list(search)
    """

    def __init__(self, model, client=None, url=None, includes=True):
        self._url = url or model._url
        self._model_cls = model
        self._request_params = {}

        self._filter_properties = None
        self._client = client or CatalogClient.get_default_client()
        self._limit = None
        self._use_includes = includes

    def limit(self, limit):
        """Limit the number of search results returned by the search execution.

        Successive calls to `limit` will overwrite the previous limit parameter.

        Parameters
        ----------
        limit : int
            The maximum number of records to return.

        Returns
        -------
        Search
        """
        s = copy.deepcopy(self)
        s._limit = limit

        return s

    def sort(self, field, ascending=True):
        """Sort the returned results by the given field.

        Multiple sort fields are not supported, so
        Successive calls to `sort` will overwrite the previous sort parameter.

        Parameters
        ----------
        field : str
            The name of the field to sort by
        ascending : bool
            Sorts results in ascending order if True and descending order if False.

        Returns
        -------
        Search

        Example
        -------
        >>> search = Search(Product).sort("created", ascending=False)
        >>> list(search)

        """
        s = copy.deepcopy(self)
        s._request_params["sort"] = ("-" if not ascending else "") + field

        return s

    def filter(self, properties):
        """Filter results by the values of various fields.

        Successive calls to `filter` will add the new filter(s) using the
        ``and`` Boolean operator (``&``).

        Parameters
        ----------
        properties : Expression
            Expression used to filter objects in the search by their properties, built
            from :class:`properties
            <descarteslabs.common.property_filtering.filtering.GenericProperties>`.
            You can construct filter expressions using the ``==``, ``!=``, ``<``,
            ``>``, ``<=`` and ``>=`` operators as well as the
            :meth:`~descarteslabs.common.property_filtering.filtering.Property.in_`
            or
            :meth:`~descarteslabs.common.property_filtering.filtering.Property.any_of`
            method.  You cannot use the boolean keywords ``and`` and ``or`` because
            of Python language limitations; instead combine filter expressions using
            ``&`` (boolean "and") and ``|`` (boolean "or").  Filters using
            :meth:`~descarteslabs.common.property_filtering.filtering.Property.like`
            are not supported.

        Raises
        ------
        ValueError
            If the properties filter provided is not supported.

        Returns
        -------
        Search
            A new :py:class:`~descarteslabs.catalog.search.Search` instance with the
            new filter(s) applied (using ``and`` if there were existing filters)

        Example
        -------
        >>> search = Search(Product).filter(
        ...     (p.resolution_min < 60) & (p.start_datetime > "2000-01-01")
        ... )
        >>> list(search)
        """
        s = copy.deepcopy(self)
        if s._filter_properties is None:
            s._filter_properties = properties
        else:
            s._filter_properties = s._filter_properties & properties
        return s

    def _serialize_filters(self):
        filters = []

        if self._filter_properties:
            serialized = self._filter_properties.jsonapi_serialize(self._model_cls)
            # Flatten top-level "and" expressions since they are fairly common, e.g.
            # if you call filter() multiple times.
            if type(self._filter_properties) == AndExpression:
                for f in serialized["and"]:
                    filters.append(f)
            else:
                filters.append(serialized)

        return filters

    def find_text(self, text):
        """Full-text search for a string in the name or description of the item.

        Not all attributes support full-text search; typically `name` and `description`
        support full-text search, except for bands and images where the name does not
        support full-text search and only bands have a description.  Successive calls
        to `find_text` override the previous find_text parameter.

        Parameters
        ----------
        text : str
            A string you want to perform a full-text search for.

        Returns
        -------
        Search
            A new instance of the :py:class:`~descarteslabs.catalog.search.Search`
            class that includes the text query.
        """
        s = copy.deepcopy(self)
        s._request_params["text"] = text
        return s

    def _to_request(self):
        s = copy.deepcopy(self)

        if self._limit is not None:
            s._request_params["limit"] = self._limit

        filters = s._serialize_filters()
        if filters:
            # urlencode encodes spaces in the json object which create an invalid filter value when
            # the server tries to parse it, so we have to remove spaces prior to encoding.
            s._request_params["filter"] = json.dumps(filters, separators=(",", ":"))

        if self._use_includes and self._model_cls._default_includes:
            s._request_params["include"] = ",".join(self._model_cls._default_includes)

        return self._url, s._request_params

    def count(self):
        """Fetch the number of documents that match the search.

        Note that this may not be an exact count if searching within a geometry.

        Returns
        -------
        int
            Number of matching records

        Raises
        ------
        BadRequestError
            If any of the query parameters or filters are invalid

        Example
        -------
        >>> count = Search(Band).filter(p.type=="spectral").count()
        """

        # modify query to return 0 results, and just get the object count
        s = self.limit(0)
        url, params = s._to_request()
        r = self._client.session.put(url, json=params)
        response = r.json()
        return response["meta"]["count"]

    def __iter__(self):
        """
        Execute the search query and get a generator for iterating through the returned results

        Returns
        -------
        generator
            Generator of objects that match the type of document being searched. Empty if no matching images found.

        Raises
        ------
        BadRequestError
            If any of the query parameters or filters are invalid

        Example
        -------
        >>> search = Search(Product).filter(p.tags == "test")
        >>> list(search)

        """
        url_next, params = self._to_request()
        while url_next is not None:
            r = self._client.session.put(url_next, json=params)
            response = r.json()
            if not response["data"]:
                break

            related_objects = self._model_cls._load_related_objects(
                response, self._client
            )

            for doc in response["data"]:
                model_class = self._model_cls._get_model_class(doc)
                yield model_class(
                    id=doc["id"],
                    client=self._client,
                    _saved=True,
                    _related_objects=related_objects,
                    **doc["attributes"]
                )

            next_link = response["links"].get("next")
            if next_link is not None:
                # The WrappedSession always prepends the base url, so we need to trim it from
                # this URL.
                if not next_link.startswith(self._client.base_url):
                    warnings.warn(
                        "Continuation URL '{}' does not match expected base URL '{}'".format(
                            next_link, self._client.base_url
                        )
                    )
                url_next = next_link[len(self._client.base_url) :]
            else:
                url_next = None

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k in ["_client"]:
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memo))
        return result


class ImageSearch(Search):
    # Be aware that the `|` characters below add whitespace.  The first one is needed
    # avoid the `Inheritance` section from appearing before the auto summary.
    """A search request that iterates over its search results.

    The `ImageSearch` is identical to `Search` but with a couple of summary methods.

    |

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base classes:

    * :py:class:`descarteslabs.catalog.search.Search`
    """

    _unsupported_summary_params = ["sort"]

    def intersects(self, geometry):
        """Filter images to those that intersect the given geometry.

        Successive calls to `intersects` override the previous intersection
        geometry.

        Parameters
        ----------
        geometry : shapely.geometry.base.BaseGeometry, geojson-like
            Geometry that found images must intersect.

        Returns
        -------
        Search
            A new instance of the :py:class:`~descarteslabs.catalog.search.ImageSearch`
            class that includes geometry filter.
        """
        s = copy.deepcopy(self)
        s._request_params["intersects"] = json.dumps(
            self._model_cls.serialize_attribute("geometry", geometry),
            separators=(",", ":"),
        )
        return s

    def _summary_request(self):
        # don't modify existing search params
        params = copy.deepcopy(self._request_params)

        for p in self._unsupported_summary_params:
            params.pop(p, None)

        filters = self._serialize_filters()
        if filters:
            # urlencode encodes spaces in the json object which create an invalid filter value when
            # the server tries to parse it, so we have to remove spaces prior to encoding.
            params["filter"] = json.dumps(filters, separators=(",", ":"))

        return params

    def summary(self):
        """Get summary statistics about the current `ImageSearch` query.

        Returns
        -------
        SummaryResult
            The summary statistics as a `SummaryResult` object.

        Example
        -------
        >>> s = Image
        ...     .search()
        ...     .filter(p.product_id=="landsat:LC08:01:RT:TOAR")
        ...     .summary()
        >>> print(s.count, s.bytes)
        """

        s = copy.deepcopy(self)
        summary_url = s._url + "/summary/all"

        r = self._client.session.put(summary_url, json=self._summary_request())
        response = r.json()

        return SummaryResult(**response["data"]["attributes"])

    def summary_interval(
        self,
        aggregate_date_field="acquired",
        interval="year",
        start_datetime=None,
        end_datetime=None,
    ):
        """Get summary statistics by specified datetime intervals about the current `ImageSearch` query.

        Parameters
        ----------

        aggregate_date_field : str, optional
            The date field to use for aggregating summary results over time.  Valid
            inputs are ``acquired``, ``created``, ``modified``, ``published``.
            The default is ``acquired``.
        interval : str, optional
            The time interval to use for aggregating summary results.  Valid inputs
            are ``year``, ``quarter``, ``month``, ``week``, ``day``, ``hour``,
            ``minute``.  The default is ``year``.
        start_datetime : str or datetime, optional
            Beginning of the date range over which to summarize data in ISO format.
            The default is any start_datetime (unbounded).
        end_datetime : str or datetime, optional
            End of the date range over which to summarize data in ISO format.
            The default is any end_datetime (unbounded).

        Returns
        -------
        list(SummaryResult)
            The summary statistics for each interval, as a list of `SummaryResult`
            objects.

        Example
        -------
        >>> s = Image
        ...     .search()
        ...     .filter(p.product_id=="landsat:LC08:01:RT:TOAR")
        ...     .summary_interval(aggregate_date_field="acquired", interval="month)
        >>> print([(i.interval_start, i.count) for i in interval_results])
        """
        s = copy.deepcopy(self)
        summary_url = s._url + "/summary"

        summary_url = "{}/{}/{}".format(summary_url, aggregate_date_field, interval)
        if start_datetime:
            s._request_params["_start"] = serialize_datetime(start_datetime)
        if end_datetime:
            s._request_params["_end"] = serialize_datetime(end_datetime)

        r = self._client.session.put(summary_url, json=s._summary_request())
        response = r.json()

        return [SummaryResult(**d["attributes"]) for d in response["data"]]


class SummaryResult(object):
    """
    The readonly data returned by :py:meth:`ImageSearch.summary` or
    :py:meth:`ImageSearch.summary_interval`.

    Attributes
    ----------
    count : int
        Number of images in the summary.
    bytes : int
        Total number of bytes of data across all images in the summary.
    products : list(str)
        List of IDs for the products included in the summary.
    interval_start: datetime
        For interval summaries only, a datetime representing the start of the interval period.

    """

    def __init__(
        self, count=None, bytes=None, products=None, interval_start=None, **kwargs
    ):
        self.count = count
        self.bytes = bytes
        self.products = products
        self.interval_start = (
            parse_iso_datetime(interval_start) if interval_start else None
        )

    def __repr__(self):
        text = [
            "\nSummary for {} images:".format(self.count),
            " - Total bytes: {:,}".format(self.bytes),
        ]
        if self.products:
            text.append(" - Products: {}".format(", ".join(self.products)))
        if self.interval_start:
            text.append(" - Interval start: {}".format(self.interval_start))
        return "\n".join(text)
