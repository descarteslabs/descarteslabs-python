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


class Expression(object):
    """An expression for filtering a property against a value or set of values.

    An expression contains a :py:class:`Property`, a comparison operator, and a value (or set of values):

        | ``property`` ``operator`` ``value``
        | or
        | ``value`` ``operator`` ``property``

    where the operator can be

    * ==
    * !=
    * <
    * <=
    * >
    * >=

    If the operator is ``<``, ``<=``, ``>`` or ``>=``, you can construct a range using

        ``value`` ``operator`` ``property`` ``operator`` ``value``

    Expressions can be combined using the Boolean operators ``and`` or ``or``,
    but due to language limitations
    the operator for ``and`` is expressed as ``&`` and the operator for ``or`` is
    expressed as ``|``.

    In addition there are a couple of method-like operators that can be used on a
    property:

    * :meth:`~descarteslabs.common.property_filtering.filtering.Property.like`
    * :meth:`~descarteslabs.common.property_filtering.filtering.Property.any_of` or
      :meth:`~descarteslabs.common.property_filtering.filtering.Property.in_`

    Example
    -------
    >>> from descarteslabs.common.property_filtering import GenericProperties
    >>> p = GenericProperties()
    >>> e = p.foo == 5
    >>> type(e)
    <class 'descarteslabs.common.property_filtering.filtering.EqExpression'>
    >>> e = p.foo.any_of([1, 2, 3, 4, 5])
    >>> type(e)
    <class 'descarteslabs.common.property_filtering.filtering.OrExpression'>
    >>> e = 5 < p.foo < 10
    >>> type(e)
    <class 'descarteslabs.common.property_filtering.filtering.RangeExpression'>
    >>> e = (5 < p.foo < 10) & p.foo.any_of([1, 2, 3, 4, 5])
    >>> type(e)
    <class 'descarteslabs.common.property_filtering.filtering.AndExpression'>

    """

    def __and__(self, other):
        return AndExpression([self]) & other

    def __or__(self, other):
        return OrExpression([self]) | other

    def __rand__(self, other):
        return AndExpression([other]) & self

    def __ror__(self, other):
        return OrExpression([other]) | self

    def _convert_name_value_pair(self, name, value):
        if hasattr(value, "id"):
            return name + "_id", value.id
        else:
            return name, value


# A convention was added to allow for serialization of catalog V2 attributes
# If a model is given, the model class method `_serialize_filter_attribute` will be
# called to retrieve the serialized value of an attribute.

# A second convention was added to allow for Catalog V2 object to be used
# instead of the name for == and != operations, and to convert
# that into the `id` field of the object.


class EqExpression(Expression):
    def __init__(self, name, value):
        self.name, self.value = self._convert_name_value_pair(name, value)

    def serialize(self):
        return {"eq": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        value = (
            model._serialize_filter_attribute(self.name, self.value)
            if model
            else self.value
        )
        return {"op": "eq", "name": self.name, "val": value}


class NeExpression(Expression):
    def __init__(self, name, value):
        self.name, self.value = self._convert_name_value_pair(name, value)

    def serialize(self):
        return {"ne": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        value = (
            model._serialize_filter_attribute(self.name, self.value)
            if model
            else self.value
        )
        return {"op": "ne", "name": self.name, "val": value}


class RangeExpression(Expression):
    def __init__(self, name, parts):
        self.name = name
        self.parts = parts

    def serialize(self):
        return {"range": {self.name: self.parts}}

    def jsonapi_serialize(self, model=None):
        serialized = [
            {
                "name": self.name,
                "op": op,
                "val": model._serialize_filter_attribute(self.name, val)
                if model
                else val,
            }
            for (op, val) in self.parts.items()
        ]
        return serialized[0] if len(serialized) == 1 else {"and": serialized}


class LikeExpression(Expression):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def serialize(self):
        return {"like": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        value = (
            model._serialize_attribute(self.name, self.value) if model else self.value
        )
        return {"name": self.name, "op": "ilike", "val": value}


class AndExpression(object):
    def __init__(self, parts):
        self.parts = parts

    def __and__(self, other):
        if isinstance(other, AndExpression):
            self.parts.extend(other.parts)
            return self
        if isinstance(other, (OrExpression, Expression)):
            self.parts.append(other)
            return self
        else:
            raise Exception("Invalid sub-expression")

    __rand__ = __and__

    def __repr__(self):
        return "<AndExpression {}>".format(self.parts)

    def serialize(self):
        return {"and": [x.serialize() for x in self.parts]}

    def jsonapi_serialize(self, model=None):
        return {"and": [part.jsonapi_serialize(model=model) for part in self.parts]}


class OrExpression(object):
    def __init__(self, parts):
        self.parts = parts

    def __or__(self, other):
        if isinstance(other, OrExpression):
            self.parts.extend(other.parts)
            return self
        if isinstance(other, (AndExpression, Expression)):
            self.parts.append(other)
            return self
        else:
            raise Exception("Invalid sub-expression")

    __ror__ = __or__

    def __repr__(self):
        return "<OrExpression {}>".format(self.parts)

    def serialize(self):
        return {"or": [x.serialize() for x in self.parts]}

    def jsonapi_serialize(self, model=None):
        return {"or": [part.jsonapi_serialize(model=model) for part in self.parts]}


def range_expr(op):
    def f(self, other):
        # This is a hack to support compound comparisons
        # such as 10 < a < 20
        self.parts[op] = other
        return RangeExpression(self.name, self.parts.copy())

    return f


class Property(object):
    """A wrapper object for a single property"""

    def __init__(self, name, parts=None):
        self.name = name
        self.parts = parts or {}

    __ge__ = range_expr("gte")
    __gt__ = range_expr("gt")
    __le__ = range_expr("lte")
    __lt__ = range_expr("lt")

    def __eq__(self, other):
        return EqExpression(self.name, other)

    def __ne__(self, other):
        return NeExpression(self.name, other)

    def __repr__(self):
        return "<Property {}>".format(self.name)

    def like(self, other):
        """Compare against a wildcard string.

        This allows for wildcards, e.g. ``like("bar%foo")`` where any
        string that starts with ``'bar'`` and ends with ``'foo'`` will be
        matched.

        This uses the SQL ``LIKE`` syntax with single character
        wildcard ``'_'`` and arbitrary character wildcard ``'%'``.

        To escape either of these wilcard characters prepend it
        with a backslash, which becomes a double backslash in the
        python string, i.e. use ``like("bar\\\\%foo")`` to match exactly
        ``'bar%foo'``.
        """
        return LikeExpression(self.name, other)

    def in_(self, iterable):
        """
        Asserts that this property must have a value equal to one of the
        values in the given iterable. This can be thought of as behaving
        like an ``in`` expression in Python or an ``IN`` expression in SQL.
        """
        return OrExpression([(self == item) for item in iterable])

    any_of = in_


class Properties(object):
    """A wrapper object to allow constructing filter expressions using properties"""

    def __init__(self, *args):
        self.props = args

    def __getattr__(self, attr):
        if attr in self.props:
            return Property(attr)

        raise AttributeError("'Properties' object has no attribute '{}'".format(attr))


class GenericProperties(object):
    """A wrapper object to allow constructing filter expressions using properties.

    You can construct filter expression using the ``==``, ``!=``, ``<``, ``>``,
    ``<=`` and ``>=`` operators as well as the
    :meth:`~descarteslabs.common.property_filtering.filtering.Property.like`
    and :meth:`~descarteslabs.common.property_filtering.filtering.Property.in_`
    or :meth:`~descarteslabs.common.property_filtering.filtering.Property.any_of`
    method. You cannot use the boolean keywords ``and`` and ``or`` because of
    Python language limitations; instead you can combine filter expressions
    with ``&`` (boolean "and") and ``|`` (boolean "or").
    """

    def __getattr__(self, attr):
        # keep sphinx happy
        if attr == "__qualname__":
            return self.__class__.__qualname__
        return Property(attr)
