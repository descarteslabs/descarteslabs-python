import sys

import six

from descarteslabs.common.graft import client

from ...cereal import serializable
from ..core import GenericProxytype, ProxyTypeError
from ..primitives import Any
from ..identifier import identifier
from ..proxify import proxify

if sys.version_info[:2] >= (3, 5):
    # `inspect.BoundArguments.apply_defaults()` was added in Python 3.5,
    # so we use the backport for any older versions, even early 3.x versions
    # that do include `inspect.signature`
    from inspect import signature
else:
    # backport for Python <= 3.5
    from descarteslabs.third_party.funcsigs.funcsigs import signature


def _promote_arg(value, arg_type, arg_name, func_name):
    try:
        return arg_type._promote(value)
    except ProxyTypeError as e:
        raise ProxyTypeError(
            "Expected {} for argument {!r} to {}, but got {!r}. {}".format(
                arg_type, arg_name, func_name, value, e
            )
        )


@serializable()
class Function(GenericProxytype):
    """
    ``Function[arg_type, ..., {kwarg: type, ...}, return_type]``: Proxy function with args, kwargs,
    and return values of specific types.

    Can be instantiated from any Python callable or string function name.

    Examples
    --------
    >>> from descarteslabs.workflows import Bool, Int, Float, Function
    >>> func_type = Function[Int, {}, Int] # function with Int arg, no kwargs, returning an Int
    >>> func_type = Function[Int, {'x': Float}, Bool] # function with Int arg, kwarg 'x' of type Float, returning a Bool
    >>> func_type = Function[{}, Int] # zero-argument function, returning a Int
    >>> my_func = Function[Int, {}, Int](lambda num: num + 5) # function to add 5 to a number
    >>> my_func(10) # will result in 15 when computed
    <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
    """

    def __init__(self, function):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic Function; the parameter and return types must be specified".format()
            )
        if isinstance(function, six.string_types):
            self.function = function
        elif callable(function):
            arg_types, kwargs_types, return_type = (
                self._type_params[:-2],
                self._type_params[-2],
                self._type_params[-1],
            )
            if len(kwargs_types) > 0:
                raise TypeError(
                    "Cannot create a Function with optional arguments from a Python function, "
                    "since optional arguments or conditionals can't be represented with graft. "
                    "You must delay Python functions into Proxtype Functions that only have positional arguments."
                )
            self.function = self._delay(function, return_type, *arg_types)
            self.graft = self.function.graft
        else:
            raise ProxyTypeError(
                "Function must be a Python callable or string name, "
                "not {}".format(function)
            )

    def __call__(self, *args, **kwargs):
        arg_types, kwargs_types, return_type = (
            self._type_params[:-2],
            self._type_params[-2],
            self._type_params[-1],
        )
        func_name = type(self).__name__

        if len(args) != len(arg_types):
            msg = "{} takes exactly {} positional argument{} ({} given)".format(
                func_name, len(arg_types), "s" if len(arg_types) != 1 else "", len(args)
            )
            if len(kwargs_types) > 0 and len(args) > len(arg_types):
                msg += ". Keyword arguments must be given by name, not positionally; did you mean to do that?"
            raise ProxyTypeError(msg)

        # NOTE(gabe): we allow missing keyword arguments, since builtin
        # functions may support them. However, users are prevented from
        # delaying a Python function with keyword arguments, since we can't
        # do default argument values, so there's not really any use.
        # The only downside is lack of named positional arguments in that case.

        unexpected_args = six.viewkeys(kwargs) - six.viewkeys(kwargs_types)
        if len(unexpected_args) > 0:
            raise ProxyTypeError(
                "Unexpected keyword argument{} {} for {}".format(
                    "s" if len(unexpected_args) > 1 else "",
                    ", ".join(six.moves.map(repr, sorted(unexpected_args))),
                    func_name,
                )
            )

        promoted_args = tuple(
            _promote_arg(value, arg_type, i, func_name)
            for i, (value, arg_type) in enumerate(zip(args, arg_types))
        )

        promoted_kwargs = {
            arg_name: _promote_arg(
                arg_value, kwargs_types[arg_name], arg_name, func_name
            )
            for arg_name, arg_value in six.iteritems(kwargs)
        }

        return return_type._from_apply(self.function, *promoted_args, **promoted_kwargs)

    @classmethod
    def _from_graft(cls, graft):
        # Necessary to have this custom initializer because we store `.function`
        # separately from `.graft`, since the function could just be a string,
        # which isn't a valid graft.
        new = super(Function, cls)._from_graft(graft)  # validate graft
        new.function = new.graft
        return new

    @classmethod
    def from_callable(cls, func, *arg_types):
        """
        Construct a Workflows Function from a Python callable.

        Parameters
        ----------
        func: Python callable
        *arg_types: ProxyType
            For each parameter of ``func``, the type that it should accept.
            The number of argument types given much match the number of arguments ``func`` actually accepts.
            If not given, the number of parameters is inferred from the function's signature,
            and `Any` is the type used for each.

        Returns
        -------
        ~descarteslabs.function.Function
        """
        # TODO(gabe): use type annotations for great good!
        if len(arg_types) == 0:
            func_signature = signature(func)
            arg_types = (Any,) * len(func_signature.parameters)
        result = cls._delay(func, None, *arg_types)
        result_type = type(result)

        concrete_type = cls[arg_types + ({}, result_type)]
        return concrete_type._from_graft(result.graft)

    # NOTE(gabe): this method will inherently fail to describe functions that return literals,
    # since if you just return a literal value from `func` that didn't interact with
    # the dummy parameters at all, there's no way for us to trace the dependency on the params
    # and therefore generate a proper function graft. A context manager system that logs
    # nested scope might be a better way to go for that reason, plus common subexpressions.
    @staticmethod
    def _delay(func, returns, *expected_arg_types):
        """
        Turn a Python function into a Proxytype object representing its logic.

        The logic of ``func`` is captured by passing dummy Proxytype objects through it
        (parameter references, cast to instances of ``argtypes``) and seeing
        what comes out the other end. Whatever operations ``func`` does on these arguments
        will be captured in their graft (or possibly cause an error, if invalid operations
        are done to the arguments), so the final value returned by ``func`` will have a
        graft representing equivalent logic to ``func``.

        Note that this won't work correctly if ``func`` uses control flow (conditionals),
        or is a non-pure function; i.e. calling ``func`` with the same arguments can produce
        different results. Closures (referencing names defined outside of ``func``) will work,
        but scope won't be quite captured correctly in the resulting graft, since those closed-over
        values will end up inside the scope of the function, instead of outside where they should be.

        Parameters
        ----------
        func: callable
            Python callable
        returns: Proxytype or None
            The return value of the function is promoted to this type.
            If promotion fails, raises an error.
            If None, no promotion is attempted, and whatever ``func`` returned
            is returned from ``_delay``.
        *expected_arg_types: Proxytype
            Types of each positional argument to ``func``.
            An instance of each is passed into ``func``.
            If none are given, ``func`` will be called with an instance of `Any`
            for each argument it takes.

        Returns
        -------
        result: instance of ``returns``
            A delayed-like object representing the logic of ``func``,
            with a graph that contains parameters
        """
        if not callable(func):
            raise TypeError(
                "Expected a Python callable object to delay, not {!r}".format(func)
            )

        func_signature = signature(func)

        if len(expected_arg_types) == 0:
            expected_arg_types = (Any,) * len(func_signature.parameters)

        # this will raise TypeError if the expected arguments
        # aren't compatible with the signature for `func`
        bound_expected_args = func_signature.bind(*expected_arg_types).arguments

        args = {
            name: identifier(name, type_)
            for name, type_ in six.iteritems(bound_expected_args)
        }

        first_guid = client.guid()
        result = func(**args)

        if returns is not None:
            try:
                result = returns._promote(result)
            except ProxyTypeError as e:
                raise ProxyTypeError(
                    "Cannot promote {} to {}, the expected return type of the function: {}".format(
                        result, returns.__name__, e
                    )
                )
        else:
            result = proxify(result)

        return type(result)._from_graft(
            client.function_graft(
                result, *tuple(func_signature.parameters), first_guid=first_guid
            )
        )
