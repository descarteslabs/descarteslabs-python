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


import functools
import re


class Expression(object):
    """An expression is the result of a filtering operation.

    An expression can contain a :py:class:`Property`, a comparison operator, and a
    value (or set of values):

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

    Expressions can be combined using the Boolean operators ``&`` and ``|`` to form
    larger expressions. Due to language limitations
    the operator for ``and`` is expressed as ``&`` and the operator for ``or`` is
    expressed as ``|``. Also, because of operator precedence, you must bracket
    expressions with ``(`` and ``)`` to avoid unexpected behavior:

        ``(`` ``property`` ``operator`` ``value`` ``)`` ``&`` ``(`` ``value`` ``operator`` ``property`` ``)``

    In addition there is a method-like operator that can be used on a
    property.

    * :py:meth:`Property.any_of` or :meth:`Property.in_`

    And a couple of properties that allow you to verify whether a property value has
    been set or not. A property value is considered ``null`` when it's either set to
    ``None`` or to the empty list ``[]`` in case of a list property. These are only
    available for the Catalog Service.

    * :py:attr:`Property.isnull`
    * :py:attr:`Property.isnotnull`

    Examples
    --------
    >>> from descarteslabs.common.property_filtering import Properties
    >>> p = Properties()
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
    >>> e = p.foo.isnotnull
    >>> type(e)
    <class 'descarteslabs.common.property_filtering.filtering.IsNotNullExpression'>
    >>> e = p.foo.isnull
    >>> type(e)
    <class 'descarteslabs.common.property_filtering.filtering.IsNullExpression'>
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

    def jsonapi_serialize(self, model=None):
        raise NotImplementedError


# A convention was added to allow for serialization of catalog V2 attributes
# If a model is given, the model class method `_serialize_filter_attribute` will be
# called to retrieve the serialized value of an attribute.

# A second convention was added to allow for Catalog V2 object to be used
# instead of the name for == and != operations, and to convert
# that into the `id` field of the object.


class EqExpression(Expression):
    """Whether a property value is equal to the given value."""

    def __init__(self, name, value):
        self.name, self.value = self._convert_name_value_pair(name, value)

    def serialize(self):
        return {"eq": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        name, value = (
            model._serialize_filter_attribute(self.name, self.value)
            if model
            else (self.name, self.value)
        )
        return {"op": "eq", "name": name, "val": value}

    def evaluate(self, obj):
        return getattr(obj, self.name) == self.value


class NeExpression(Expression):
    """Whether a property value is not equal to the given value."""

    def __init__(self, name, value):
        self.name, self.value = self._convert_name_value_pair(name, value)

    def serialize(self):
        return {"ne": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        name, value = (
            model._serialize_filter_attribute(self.name, self.value)
            if model
            else (self.name, self.value)
        )
        return {"op": "ne", "name": name, "val": value}

    def evaluate(self, obj):
        return getattr(obj, self.name) != self.value


class RangeExpression(Expression):
    """Whether a property value is within the given range.

    A range can have a single value that must be ``>``, ``>=``,
    ``<`` or ``<=`` than the value of the property. If the range
    has two values, the property value must be between the given
    range values.
    """

    def __init__(self, name, parts):
        self.name = name
        self.parts = parts

    def serialize(self):
        return {"range": {self.name: self.parts}}

    def jsonapi_serialize(self, model=None):
        serialized = []
        for op, val in self.parts.items():
            name, value = (
                model._serialize_filter_attribute(self.name, val)
                if model
                else (self.name, val)
            )
            serialized.append({"name": name, "op": op, "val": value})
        return serialized[0] if len(serialized) == 1 else {"and": serialized}

    def evaluate(self, obj):
        result = True

        for op, val in self.parts.items():
            if op == "gte":
                result = result and getattr(obj, self.name) >= val
            elif op == "gt":
                result = result and getattr(obj, self.name) > val
            elif op == "lte":
                result = result and getattr(obj, self.name) <= val
            elif op == "lt":
                result = result and getattr(obj, self.name) < val
            else:
                raise ValueError("Unknown operation")

        return result


class IsNullExpression(Expression):
    """Whether a property value is ``None`` or ``[]``."""

    def __init__(self, name):
        self.name = name

    def serialize(self):
        raise TypeError("'isnull' expression is not supported")
        # return {"isnull": self.name}

    def jsonapi_serialize(self, model=None):
        name = self.name

        if model:
            name, _ = model._serialize_filter_attribute(self.name, None)

        return {"name": name, "op": "isnull"}

    def evaluate(self, obj):
        return getattr(obj, self.name) is None


class IsNotNullExpression(Expression):
    """Whether a property value is not ``None`` or ``[]``."""

    def __init__(self, name):
        self.name = name

    def serialize(self):
        raise TypeError("'isnotnull' expression is not supported")
        # return {"isnotnull": self.name}

    def jsonapi_serialize(self, model=None):
        name = self.name

        if model:
            name, _ = model._serialize_filter_attribute(self.name, None)

        return {"name": name, "op": "isnotnull"}

    def evaluate(self, obj):
        return getattr(obj, self.name) is not None


class PrefixExpression(Expression):
    """Whether a string property value starts with the given string prefix."""

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def serialize(self):
        return {"prefix": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        if model:
            name, _ = model._serialize_filter_attribute(self.name, None)

        return {"op": "prefix", "name": name, "val": self.value}

    def evaluate(self, obj):
        return getattr(obj, self.name).startswith(self.value)


class LikeExpression(Expression):
    """Whether a property value matches the given wildcard expression.

    The wildcard expression can contain ``%`` for zero or more characters and
    ``_`` for a single character.

    This expression is not supported by the `Catalog` service.
    """

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def serialize(self):
        return {"like": {self.name: self.value}}

    def jsonapi_serialize(self, model=None):
        name, value = (
            model._serialize_filter_attribute(self.name, self.value)
            if model
            else (self.name, self.value)
        )
        return {"name": name, "op": "ilike", "val": value}

    def evaluate(self, obj):
        expr = re.escape(self.value).replace("_", ".").replace("%", ".*")
        return re.match(f"^{expr}$", getattr(obj, self.name)) is not None


class LogicalExpression(object):
    def __init__(self, parts):
        self.parts = parts


class AndExpression(LogicalExpression):
    """``True`` if both expressions are ``True``, ``False`` otherwise."""

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

    def evaluate(self, obj):
        for part in self.parts:
            if not part.evaluate(obj):
                return False

        return True


class OrExpression(LogicalExpression):
    """``True`` if either expression is ``True``, ``False`` otherwise."""

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

    def evaluate(self, obj):
        for part in self.parts:
            if part.evaluate(obj):
                return True

        return False


def range_expr(op):
    def f(self, other):
        # This is a hack to support compound comparisons
        # such as 10 < a < 20
        self.parts[op] = other
        return RangeExpression(self.name, self.parts.copy())

    return f


def check_can_filter(fn):
    """Decorator to check whether a property can be filtered on.

    This is used by Documents in some object oriented clients.
    """

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if getattr(self, "filterable", True):
            return fn(self, *args, **kwargs)
        else:
            raise ValueError(f"Cannot filter on property: {self.name}")

    return wrapper


class Property(object):
    """A filter property that can be used in an expression.

    Although you can generate filter properties by instantiating this class, a more
    convenient method is to use a
    :py:class:`~descarteslabs.common.property_filtering.filtering.Properties`
    instance.
    By referencing any attribute of a
    :py:class:`~descarteslabs.common.property_filtering.filtering.Properties`
    instance the corresponding filter property
    will be created.

    See :ref:`Properties Introduction <property_filtering>`
    for a more detailed explanation.

    Examples
    --------
    >>> e = Property("modified") > "2020-01-01"
    """

    def __init__(self, name, parts=None):
        self.name = name
        self.parts = parts or {}

    __ge__ = check_can_filter(range_expr("gte"))
    __gt__ = check_can_filter(range_expr("gt"))
    __le__ = check_can_filter(range_expr("lte"))
    __lt__ = check_can_filter(range_expr("lt"))

    @check_can_filter
    def __eq__(self, other):
        return EqExpression(self.name, other)

    @check_can_filter
    def __ne__(self, other):
        return NeExpression(self.name, other)

    @check_can_filter
    def __repr__(self):
        return "<Property {}>".format(self.name)

    @check_can_filter
    def prefix(self, prefix):
        """Compare against a prefix string."""
        return PrefixExpression(self.name, prefix)

    startswith = prefix

    @check_can_filter
    def like(self, wildcard):
        """Compare against a wildcard string.

        This can only be used in expressions for the ``Vector`` service.
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
        return LikeExpression(self.name, wildcard)

    @check_can_filter
    def any_of(self, iterable):
        """The property must have any of the given values.

        Asserts that this property must have a value equal to one of the
        values in the given iterable. This can be thought of as behaving
        like an ``in`` expression in Python or an ``IN`` expression in SQL.
        """
        exprs = [(self == item) for item in iterable]

        if len(exprs) > 1:
            return OrExpression(exprs)
        elif len(exprs) == 1:
            return exprs[0]
        else:
            # technically we should return an expression that always evaluates false
            # (to match python in operator on an empty sequence). But there's nothing
            # we can do here to create such an expression, so instead error to the user
            # since they surely didn't mean this.
            raise ValueError("in_ expression requires at least one item")

    in_ = any_of

    @property
    @check_can_filter
    def isnull(self):
        """Whether a property value is ``None`` or ``[]``.

        This can only be used in expressions for the ``Catalog`` service.
        """
        return IsNullExpression(self.name)

    @property
    @check_can_filter
    def isnotnull(self):
        """Whether a property value is not ``None`` or ``[]``.

        This can only be used in expressions for the ``Catalog`` service.
        """
        return IsNotNullExpression(self.name)


class Properties(object):
    """A wrapper object to construct filter properties by referencing instance attributes.

    By referring to any instance attribute, a corresponding property will be created.
    The instance validates whether the generated property is in the list of property
    names that this instance was created with.

    See :ref:`Properties Introduction <property_filtering>`
    for a more detailed explanation.

    Parameters
    ----------
    name: str
        The property names that are allowed, each as a positional parameter.

    Examples
    --------
    >>> p = Properties("modified", "created")
    >>> e = p.modified > "2020-01-01"
    >>> e = p.deleted > "2020-01-01"  # doctest: +SKIP
    Traceback (most recent call last):
      ...
    AttributeError: 'Properties' object has no attribute 'deleted'
    >>>"""

    def __init__(self, *args):
        self.props = args

    def __getattr__(self, attr):
        # keep sphinx happy
        if attr == "__qualname__":
            return self.__class__.__qualname__

        if not self.props:
            # implement the old GenericProperties
            return Property(attr)

        if attr in self.props:
            return Property(attr)

        raise AttributeError("'Properties' object has no attribute '{}'".format(attr))
