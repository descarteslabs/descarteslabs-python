# © 2025 EarthDaily Analytics Corp.
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
import inspect
import json
import re
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union

AnyExpression = TypeVar("AnyExpression", bound="Expression")


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

    __abstract__: bool = False
    _aliases: List[str] = None
    _registry: Dict[str, Type[AnyExpression]] = dict()
    _operator: str = None

    def __init_subclass__(cls) -> None:
        # Do not register base classes
        if cls.__dict__.get("__abstract__", False):
            return

        operator = cls._operator

        if not operator:
            operator = cls.__name__.replace("Expression", "").lower()
            setattr(cls, "_operator", operator)

        to_add = cls._aliases or []
        to_add.append(operator)

        # Register the operator and all aliases
        for operator in to_add:
            if operator in cls._registry:
                other_expression = cls._registry[operator]

                raise ValueError(
                    "Expression {} already exists with operator {}".format(
                        other_expression, operator
                    )
                )

            cls._registry[operator] = cls

    def jsonapi_serialize(self, model=None):
        raise NotImplementedError

    def is_same(self, other: Any) -> bool:
        """Determine if two expressions are the same. This is different
        from testing for equivalence (eg `a == b` and `b == a` are equivalent,
        bit not the same).
        """
        return type(self) is type(other)

    @classmethod
    def parse(
        cls, data: Union[str, Dict[str, Any], List[Dict[str, Any]]]
    ) -> AnyExpression:
        """Parses a serialized filter into a series of expression objects.

        Parameters
        ----------
        data: str or dict
            The serialized filter expression. This can be a JSON string or a dict.
        """
        if isinstance(data, str):
            data = json.loads(data)

        if not isinstance(data, (list, dict)):
            raise ValueError("Invalid filter expression")

        if isinstance(data, list):
            return AndExpression([Expression._parse_filter_part(item) for item in data])

        return Expression._parse_filter_part(data)

    @classmethod
    def _parse_filter_part(cls, data: Dict[str, Any]) -> AnyExpression:
        """Parses a single filter expression."""
        op, value = cls._parse_operator(data)

        if op in cls._registry:
            return cls._registry[op]._parse(value)
        else:
            raise ValueError(f"Unknown filter operator: {op}")

    @classmethod
    def _parse_operator(self, data: Dict[str, Any]):
        if not isinstance(data, dict):
            raise ValueError(f"Invalid filter expected dict found: {data}")

        op = data.get("op")

        if op:
            # This is a json api expression
            return op, data
        else:
            # This is a standard expression
            if len(data.keys()) == 1:
                op = list(data.keys())[0]
                value = data[op]

                return op, value
            else:
                raise ValueError(f"Invalid filter expression: {data}")

    @classmethod
    def _parse(cls, *args) -> AnyExpression:
        """Parse the input into a specific expression type.

        Must be implemented by subclasses.
        """
        raise NotImplementedError(f"{cls.__name__}: {args}")


class OpExpression(Expression):
    """Base class for expressions that have an operator and a value."""

    __abstract__ = True

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return self.name == other.name

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

    @classmethod
    def _parse(cls, data: Dict[str, Any]) -> AnyExpression:
        signature = inspect.signature(cls)
        num_params = len(signature.parameters)

        if isinstance(data, dict) and "op" in data:
            # Json api expression
            name, val = cls._parse_jsonapi_filter(data)
        else:
            # Standard expression
            name, val = cls._parse_filter(data)

        if name is None:
            raise ValueError(
                f"Invalid {cls._operator} expression missing field name: {data}"
            )

        # Some expressions only take the name and no value
        if num_params == 1:
            return cls(name)
        else:
            if val is None:
                raise ValueError(
                    f"Invalid {cls._operator} expression missing value: {data}"
                )

            return cls(name, val)

    @classmethod
    def _parse_filter(cls, data: Any) -> Tuple[str, Any]:
        if not isinstance(data, dict):
            raise ValueError(
                f"Invalid {cls._operator} value expected dict found: {data}"
            )

        if len(data.keys()) != 1:
            # There must be exactly one field name
            raise ValueError(f"Invalid {cls._operator} expression: {data}")

        return list(data.items())[0]

    @classmethod
    def _parse_jsonapi_filter(cls, data: Dict[str, Any]) -> Tuple[str, Any]:
        return data.get("name"), data.get("val")


class LogicalExpression(Expression):
    """Base class for logical expressions that have sub expressions."""

    __abstract__ = True

    def __init__(self, parts):
        self.parts = parts

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return len(self.parts) == len(other.parts) and all(
            part.is_same(other_part)
            for part, other_part in zip(self.parts, other.parts)
        )

    @classmethod
    def _parse(cls, data: List[Dict[str, Any]]) -> AnyExpression:
        parts = [Expression.parse(expr) for expr in data]
        return cls(parts)


# A convention was added to allow for serialization of catalog V2 attributes
# If a model is given, the model class method `_serialize_filter_attribute` will be
# called to retrieve the serialized value of an attribute.

# A second convention was added to allow for Catalog V2 object to be used
# instead of the name for == and != operations, and to convert
# that into the `id` field of the object.


class EqExpression(OpExpression):
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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return self.value == other.value


class NeExpression(OpExpression):
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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return self.value == other.value


class RangeExpression(OpExpression):
    """Whether a property value is within the given range.

    A range can have a single value that must be ``>``, ``>=``,
    ``<`` or ``<=`` than the value of the property. If the range
    has two values, the property value must be between the given
    range values.
    """

    _aliases = ["gt", "gte", "lt", "lte"]

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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return len(self.parts) == len(other.parts) and all(
            part == other_part
            for part, other_part in zip(self.parts.items(), other.parts.items())
        )

    @classmethod
    def _parse_jsonapi_filter(cls, data: Dict[str, Any]) -> AnyExpression:
        """Override parsing to handle json api special case"""
        return data.get("name"), {data["op"]: data.get("val")}


class IsNullExpression(OpExpression):
    """Whether a property value is ``None`` or ``[]``."""

    def __init__(self, name):
        self.name = name

    def serialize(self):
        return {self._operator: self.name}

    def jsonapi_serialize(self, model=None):
        name = self.name

        if model:
            name, _ = model._serialize_filter_attribute(self.name, None)

        return {"name": name, "op": self._operator}

    def evaluate(self, obj):
        return getattr(obj, self.name) is None

    @classmethod
    def _parse_filter(cls, data: Any) -> Tuple[str, Any]:
        if not isinstance(data, str):
            raise ValueError(
                f"Invalid {cls._operator} value expected str found: {data}"
            )

        return data, None


class IsNotNullExpression(OpExpression):
    """Whether a property value is not ``None`` or ``[]``."""

    def __init__(self, name):
        self.name = name

    def serialize(self):
        return {self._operator: self.name}

    def jsonapi_serialize(self, model=None):
        name = self.name

        if model:
            name, _ = model._serialize_filter_attribute(self.name, None)

        return {"name": name, "op": self._operator}

    def evaluate(self, obj):
        return getattr(obj, self.name) is not None

    @classmethod
    def _parse_filter(cls, data: Any) -> Tuple[str, Any]:
        if not isinstance(data, str):
            raise ValueError(
                f"Invalid {cls._operator} value expected str found: {data}"
            )

        return data, None


class PrefixExpression(OpExpression):
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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return self.value == other.value


class LikeExpression(OpExpression):
    """Whether a property value matches the given wildcard expression.

    The wildcard expression can contain ``%`` for zero or more characters and
    ``_`` for a single character.

    This expression is not supported by the `Catalog` service.
    """

    _aliases = ["ilike"]

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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return self.value == other.value


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

    def __or__(self, other):
        return OrExpression([self]) | other

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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return len(self.parts) == len(other.parts) and all(
            part.is_same(other_part)
            for part, other_part in zip(self.parts, other.parts)
        )


class OrExpression(LogicalExpression):
    """``True`` if either expression is ``True``, ``False`` otherwise."""

    def __and__(self, other):
        return AndExpression([self]) & other

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

    def is_same(self, other: Any) -> bool:
        if not super().is_same(other):
            return False

        return len(self.parts) == len(other.parts) and all(
            part.is_same(other_part)
            for part, other_part in zip(self.parts, other.parts)
        )


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
