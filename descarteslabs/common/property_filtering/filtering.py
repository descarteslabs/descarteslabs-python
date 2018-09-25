# Copyright 2018 Descartes Labs.
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
    def __and__(self, other):
        return AndExpression([self]) & other

    def __or__(self, other):
        return OrExpression([self]) | other

    def __rand__(self, other):
        return AndExpression([other]) & self

    def __ror__(self, other):
        return OrExpression([other]) | self


class EqExpression(Expression):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def serialize(self):
        return {'eq': {self.name: self.value}}


class NeExpression(Expression):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def serialize(self):
        return {'ne': {self.name: self.value}}


class RangeExpression(Expression):
    def __init__(self, name, parts):
        self.name = name
        self.parts = parts

    def serialize(self):
        return {'range': {self.name: self.parts}}


class LikeExpression(Expression):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def serialize(self):
        return {'like': {self.name: self.value}}


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
        return '<AndExpression {}>'.format(self.parts)

    def serialize(self):
        return {'and': [x.serialize() for x in self.parts]}


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
        return '<OrExpression {}>'.format(self.parts)

    def serialize(self):
        return {'or': [x.serialize() for x in self.parts]}


def range_expr(op):
    def f(self, other):
        # This is a hack to support compound comparisons
        # such as 10 < a < 20
        self.parts[op] = other
        return RangeExpression(self.name, self.parts.copy())
    return f


class Property(object):
    def __init__(self, name, parts=None):
        self.name = name
        self.parts = parts or {}

    __ge__ = range_expr('gte')
    __gt__ = range_expr('gt')
    __le__ = range_expr('lte')
    __lt__ = range_expr('lt')

    def __eq__(self, other):
        return EqExpression(self.name, other)

    def __ne__(self, other):
        return NeExpression(self.name, other)

    def __repr__(self):
        return '<Property {}>'.format(self.name)

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


class Properties(object):
    def __init__(self, *args):
        self.props = args

    def __getattr__(self, attr):
        if attr in self.props:
            return Property(attr)

        raise AttributeError(
            "'Properties' object has no attribute '{}'".format(attr))


class GenericProperties(object):
    def __getattr__(self, attr):
        return Property(attr)
