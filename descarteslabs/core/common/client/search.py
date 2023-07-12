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

import copy
import json
from typing import TYPE_CHECKING, Generic, Iterator, List, TypeVar, Union

from ..collection import Collection
from ..property_filtering.filtering import AndExpression, Expression, LogicalExpression
from .attributes import Attribute
from .sort import Sort

if TYPE_CHECKING:
    from ...client.services.service import ApiService

T = TypeVar("T")


class Search(Generic[T]):
    """A search request that iterates over its search results.

    The search can be narrowed by using the methods on the search object.

    Example
    -------
    >>> search = Search(Model).filter(Model.name == "test")
    >>> list(search) # doctest: +SKIP
    >>> search.collect() # doctest: +SKIP
    """

    def __init__(self, document: T, client: "ApiService", url: str = None, **params):
        self._document = document
        self._client = client
        self._url = url or document._url

        self._filters: Expression = None
        self._limit: int = None
        self._sort: List[Sort] = []
        self._params: dict = params

    def __deepcopy__(self, memo):
        """Override to avoid deep copying the client"""
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in self.__dict__.items():
            if k in ["_client"]:
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memo))

        return result

    def __iter__(self) -> Iterator[T]:
        """
        Execute the search query and make a generator for iterating through the returned results

        Returns
        -------
        generator
            Generator of objects that match the type of document being searched.
            Empty if no matching documents found.

        Raises
        ------
        BadRequestError
            If any of the query parameters or filters are invalid
        ~descarteslabs.exceptions.ClientError or ~descarteslabs.exceptions.ServerError
            :ref:`Spurious exception <network_exceptions>` that can occur during a
            network request.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> search = Function.search().filter(Function.status == "success")
        >>> list(search) # doctest: +SKIP
        """
        documents = self._client.iter_pages(self._url, params=self._serialize())

        for document in documents:
            yield self._document(**document, saved=True)

    def collect(self, **kwargs) -> Collection[T]:
        """
        Execute the search query and return the appropriate collection.

        Returns
        -------
        ~descarteslabs.common.collection.Collection
            Collection of objects that match the type of document beng searched.

        Raises
        ------
        BadRequestError
            If any of the query parameters or filters are invalid
        ~descarteslabs.exceptions.ClientError or ~descarteslabs.exceptions.ServerError
            :ref:`Spurious exception <network_exceptions>` that can occur during a
            network request.
        """
        return Collection(self, item_type=self._document)

    def count(self) -> int:
        """Fetch the number of documents that match the search.

        Returns
        -------
        int
            Number of matching records

        Raises
        ------
        BadRequestError
            If any of the query parameters or filters are invalid
        ~descarteslabs.exceptions.ClientError or ~descarteslabs.exceptions.ServerError
            :ref:`Spurious exception <network_exceptions>` that can occur during a
            network request.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> search = Function.search().filter(Function.status == "building")
        >>> count = search.count() # doctest: +SKIP
        """
        instance = self.limit(0)
        response = self._client.session.get(self._url, params=instance._serialize())
        return response.json()["meta"]["total"]

    def filter(self, expression: Union[Expression, LogicalExpression]) -> "Search[T]":
        """Filter results by the values of various fields.

        Successive calls to `filter` will add the new filter(s) using the
        ``and`` Boolean operator (``&``).

        Parameters
        ----------
        expression : Expression
            Expression used to filter objects in the search by their attributes, built
            from class :class:`attributes
            <descarteslabs.common.client.attributes.Attribute>` ex. Job.id == 'some-id'.
            You can construct filter expressions using the ``==``, ``!=``, ``<``,
            ``>``, ``<=`` and ``>=`` operators as well as the
            :meth:`~descarteslabs.common.client.attributes.Attribute.in_`
            or
            :meth:`~descarteslabs.common.client.attributes.Attribute.any_of`
            method.  You cannot use the boolean keywords ``and`` and ``or`` because
            of Python language limitations; instead combine filter expressions using
            ``&`` (boolean "and") and ``|`` (boolean "or").

        Returns
        -------
        Search
            A new :py:class:`~descarteslabs.common.client.Search` instance with the
            new filter(s) applied (using ``and`` if there were existing filters)

        Raises
        ------
        ValueError
            If the filter expression provided is not supported.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> search = Job.search().filter(
        ...     (Job.runtime > 60) | (Job.status == "failure")
        ... )
        >>> list(search) # doctest: +SKIP
        """
        instance = copy.deepcopy(self)

        if not isinstance(expression, (Expression, LogicalExpression)):
            raise TypeError(
                f"Expected an Expression not: {expression.__class__.__name__}"
            )

        if instance._filters is None:
            instance._filters = expression
        else:
            instance._filters = instance._filters & expression

        return instance

    def limit(self, limit: int) -> "Search[T]":
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
        instance = copy.deepcopy(self)
        instance._limit = limit
        return instance

    def param(self, **params) -> "Search[T]":
        """Add additional parameters to the search request.

        Parameters
        ----------
        params : dict
            The parameters to add to the search request.

        Returns
        -------
        Search
        """
        instance = copy.deepcopy(self)
        instance._params.update(params)
        return instance

    def sort(self, *sorts: List[Union[Attribute, Sort]]) -> "Search[T]":
        """Sort the returned results by the given fields.

        Parameters
        ----------
        sorts : List[Union[Attribute, Sort]]
            The attributes and direction to sort by.

        Returns
        -------
        Search

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> Function.search().sort(Function.id, -Function.creation_date) # doctest: +SKIP
        >>> list(search) # doctest: +SKIP
        """
        instance = copy.deepcopy(self)

        for sort in sorts:
            if isinstance(sort, Attribute):
                sort = sort._to_sort()
            elif not isinstance(sort, Sort):
                raise TypeError(
                    f"Expected an Attribute or Sort not: {sort.__class__.__name__}"
                )

            instance._sort.append(sort)

        return instance

    def _serialize(self):
        params = self._params.copy()

        if self._filters:
            filters = []
            filter = self._filters.jsonapi_serialize(self._document)

            if type(self._filters) == AndExpression:
                for f in filter["and"]:
                    filters.append(f)
            else:
                filters.append(filter)

            params["filter"] = json.dumps(
                filters, separators=(",", ":"), sort_keys=True
            )

        if self._limit is not None:
            params["limit"] = self._limit

        if self._sort:
            params["sort"] = [sort.to_string() for sort in self._sort]

        return params

    def __repr__(self) -> str:
        return f"<Search {self._document.__name__}>"
