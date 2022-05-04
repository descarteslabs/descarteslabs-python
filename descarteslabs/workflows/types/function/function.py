import typing
import keyword

from inspect import signature, Signature, Parameter

from ....common.graft import client

from ...cereal import serializable
from ..core import (
    Proxytype,
    GenericProxytype,
    ProxyTypeError,
    assert_is_proxytype,
    type_params_issubclass,
)
from ..identifier import identifier
from ..proxify import proxify


def _promote_arg(value, arg_type, arg_name, func_name):
    try:
        return arg_type._promote(value)
    except ProxyTypeError as e:
        raise ProxyTypeError(
            "Expected {} for argument {!r} to {}, but got {!r}. {}".format(
                arg_type, arg_name, func_name, value, e
            )
        )


def _make_signature(
    arg_types: typing.Iterable[type],
    kwarg_types: typing.Dict[str, type],
    return_type: typing.Optional[type] = None,
):
    pos_only_params = [
        Parameter(f"implicit{i}", Parameter.POSITIONAL_ONLY, annotation=type_)
        for i, type_ in enumerate(arg_types)
    ]
    kwarg_params = [
        Parameter(name, Parameter.POSITIONAL_OR_KEYWORD, annotation=type_)
        for name, type_ in kwarg_types.items()
    ]

    if return_type is None:
        return_type = Parameter.empty

    return Signature(pos_only_params + kwarg_params, return_annotation=return_type)


@serializable()
class Function(GenericProxytype):
    """
    ``Function[arg_type, ..., {kwarg: type, ...}, return_type]``: Proxy function.

    You can create a `Function` from any Python function, usually using `Function.from_callable`
    (or `.proxify`). You can also turn a Workflows object that depends on `parameters <.parameter>`
    into a `Function` using `Function.from_object`.

    Functions have positional-only arguments, named arguments, and a return value of specific types.
    All the arguments are required. Like Python functions, the named arguments can be given positionally
    or by name. For example, if ``func`` is a ``Function[{'x': Int, 'y': Str}, Int]``, ``func(1, "hello")``,
    ``func(x=1, y="hello")``, and ``func(y="hello", x=1)`` are all equivalent.

    If you're creating a Function yourself, you should always use named arguments---positional-only arguments
    are primarily for internal use. Just use `Function.from_callable` or `Function.from_object` and it will take
    care of everything for you.

    ``isinstance`` and ``issubclass`` have special behavior for Functions, since unlike Python, Functions
    are strongly typed (you know what type of arguments they take, and what type of value they return, without
    having to run them). In general, if ``x`` and ``y`` are both Functions, ``isinstance(x, type(y))`` means that
    you can safely use ``x`` wherever you could use ``y``---the types they accept and return are compatible.
    Formally, `Function` is *contravariant* in its argument types and *covariant* in its return type.
    That means that a ``Function[Number, ...]`` is considered a *subtype* of ``Function[Int, ...]``,
    because any function that can handle a ``Number`` can also handle an `Int`. Whereas ``Function[... Int]``
    (`Function` that returns an `Int`) is a subtype of ``Function[..., Number]``, since `Int` is a subtype
    of ``Number``.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> @wf.Function.from_callable
    ... def pow(base: wf.Int, exp: wf.Float) -> wf.Float:
    ...     return base ** exp
    >>> pow
    <descarteslabs.workflows.types.function.function.Function[{'base': Int, 'exp': Float}, Float] object at 0x...>
    >>> pow(16, 0.5).inspect() # doctest: +SKIP
    4

    >>> word = wf.parameter("word", wf.Str)
    >>> repeats = wf.widgets.slider("repeats", min=0, max=5, step=1)
    >>> repeated = (word + " ") * repeats
    >>> repeat_func = wf.Function.from_object(repeated)
    >>> repeat_func
    <descarteslabs.workflows.types.function.function.Function[{'word': Str, 'repeats': Int}, Str] object at 0x...>
    >>> repeat_func("hello", 3).inspect() # doctest: +SKIP
    'hello hello hello '

    >>> from descarteslabs.workflows import Int, Float, Bool
    >>> func_type = Function[Int, {}, Int] # function with Int arg, no named args, returning an Int
    >>> func_type = Function[Int, {'x': Float}, Bool] # function with Int arg, kwarg 'x' of type Float, returning a Bool
    >>> func_type = Function[{}, Int] # zero-argument function, returning a Int

    >>> func = Function[Int, Int, {}, Int](lambda x, y: x + y) # function taking two Ints and adding them together
    >>> func
    <descarteslabs.workflows.types.function.function.Function[Int, Int, {}, Int] object at 0x...>
    >>> func(3, 4).inspect() # doctest: +SKIP
    7
    """

    def __init_subclass__(cls, **kwargs):
        "Add a ``__signature__`` to concrete subclasses"
        super().__init_subclass__(**kwargs)
        if cls._type_params is None:
            return

        arg_types = cls.arg_types.fget(cls)
        kwarg_types = cls.kwarg_types.fget(cls)
        return_type = cls.return_type.fget(cls)

        cls.__signature__ = _make_signature(arg_types, kwarg_types, return_type)
        cls.__annotations__ = {**kwarg_types, "return": return_type}

    def __init__(self, function):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic Function; the parameter and return types must be specified"
            )
        if isinstance(function, str):
            self.graft = client.keyref_graft(function)
            self.params = ()
        elif isinstance(function, Function):
            # If the `function` is compatible with our types (remember, `isinstance` has fancy logic to
            # check this for us) use its graft, otherwise error.
            if isinstance(function, type(self)):
                self.graft = function.graft
                self.params = function.params
            else:
                raise TypeError(
                    f"Expected a {type(self).__name__}. "
                    f"Got a {type(function).__name__}.\n"
                    "Their signatures are incompatible:\n"
                    f"need: {self.__signature__}\n"
                    f"got:  {function.__signature__}"
                )
        elif callable(function):
            result = self._delay(
                function, self.return_type, *self.arg_types, **self.kwarg_types
            )
            self.graft = result.graft
            self.params = result.params
        else:
            raise ProxyTypeError(
                "Function must be a Python callable or string name, "
                "not {}".format(function)
            )

    def __call__(self, *args, **kwargs):
        promoted_args, promoted_kwargs = self._promote_arguments(*args, **kwargs)
        return self.return_type._from_apply(self, *promoted_args, **promoted_kwargs)

    def _promote_arguments(self, *args, **kwargs) -> typing.Tuple[list, dict]:
        bound = self.__signature__.bind(*args, **kwargs)
        # ^ NOTE: raises error if args are incompatible

        func_name = type(self).__name__
        promoted_args = []
        promoted_kwargs = {}
        for i, ((name, value), param) in enumerate(
            zip(bound.arguments.items(), self.__signature__.parameters.values())
        ):
            promoted = _promote_arg(
                value,
                param.annotation,
                i if param.kind is Parameter.POSITIONAL_ONLY else name,
                func_name,
            )
            if param.kind is Parameter.POSITIONAL_ONLY:
                promoted_args.append(promoted)
            else:
                promoted_kwargs[name] = promoted

        return promoted_args, promoted_kwargs

    @property
    def arg_types(self) -> typing.Tuple[typing.Type[Proxytype]]:
        "tuple: The types of the positional-only arguments this `Function` takes"
        return self._type_params[:-2]

    @property
    def kwarg_types(self) -> typing.Dict[str, typing.Type[Proxytype]]:
        "dict: The names and types, in order, of the named arguments this `Function` takes"
        return self._type_params[-2]

    @property
    def all_arg_types(self) -> typing.Tuple[typing.Type[Proxytype]]:
        "tuple: The types of all arguments this `Function` takes, in positional order (`arg_types` + `kwarg_types`)"
        return self.arg_types + tuple(self.kwarg_types.values())

    @property
    def return_type(self) -> typing.Type[Proxytype]:
        "type: The Proxytype returned by this `Function`"
        return self._type_params[-1]

    @classmethod
    def _validate_params(cls, type_params):
        try:
            *arg_types, kwargs_types, return_type = type_params
        except ValueError:
            raise ValueError(
                "Not enough type parameters supplied to Function. Function requires 0 or more "
                "positional-only argument types, a dict of named-argument types (which is often empty), "
                "and one return type. For example, `Function[Int, Float, {'x': Int}, Int]`"
            ) from None

        # Check arg types
        for i, type_param in enumerate(arg_types):
            error_message = (
                "Function argument type parameters must be Proxytypes, "
                "but for argument parameter {}, got {}".format(i, type_param)
            )
            assert_is_proxytype(type_param, error_message=error_message)

        # Check format of kwargs and types
        assert isinstance(
            kwargs_types, dict
        ), "Function kwarg type parameters must be a dict, not {!r}".format(
            kwargs_types
        )
        for name, type_param in kwargs_types.items():
            assert isinstance(
                name, str
            ), "Keyword argument names must be strings, but '{}' is a {!r}".format(
                name, type(name)
            )
            assert name.isidentifier() and not keyword.iskeyword(
                name
            ), f"Function argument names must be valid Python identifiers; {name!r} is not."

            error_message = "Function kwarg type parameters must be Proxytypes, but for kwarg {}, got {!r}".format(
                name, type_param
            )
            assert_is_proxytype(type_param, error_message=error_message)

        # Check return type
        error_message = (
            "Function return type parameter must be a Proxytype, but got {!r}".format(
                return_type
            )
        )
        assert_is_proxytype(return_type, error_message=error_message)

    @classmethod
    def _issubclass(cls, other: typing.Type["Function"]) -> bool:
        """
        Check whether another `Function` could be used in place of this one.

        * Considers all arguments positionally, since kwargs can be given positionally.
        * For an argument that this function takes positionally, the other could take it by name
          (if the positions match).
        * For an argument that this function takes by name, the other must take it by the same name,
          in the same position (basically, kwargs must be in the same order).
        * Argument types are checked *contravariantly*: the other Function can take types that
          are *supertypes* of this one.
        """
        our_return_type = cls.return_type.fget(cls)
        other_return_type = other.return_type.fget(other)
        if not issubclass(other_return_type, our_return_type):
            # Return type is covariant: to be a subtype, `other` must return a subtype of our return type.
            return False

        our_args = cls.arg_types.fget(cls)
        our_kwargs = cls.kwarg_types.fget(cls)
        other_args = other.arg_types.fget(other)
        other_kwargs = other.kwarg_types.fget(other)

        all_our_types = our_args + tuple(our_kwargs.values())
        all_other_types = other_args + tuple(other_kwargs.values())

        if not type_params_issubclass(all_our_types, all_other_types):
            # Positional contravariance: all our arguments (considered positionally) must be
            # subtypes of their arguments. `issubclass(Function[Int], Function[SubInt])` should be
            # True: a `Function[Int]` can accept a `SubInt` perfectly well.
            return False

        all_our_names = (None,) * len(our_args) + tuple(our_kwargs)
        all_other_names = (None,) * len(other_args) + tuple(other_kwargs)

        for our_name, other_name in zip(all_our_names, all_other_names):
            if our_name is None:
                continue
            if our_name != other_name:
                return False

        return True

    @classmethod
    def from_object(cls, obj):
        """
        Turn a Workflows object that depends on parameters into a `Function`.

        Any parameters ``obj`` depends on become arguments to the `Function`.
        Calling that function essentially returns ``obj``, with the given values applied
        to those parameters.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> word = wf.parameter("word", wf.Str)
        >>> repeats = wf.widgets.slider("repeats", min=0, max=5, step=1)
        >>> repeated = (word + " ") * repeats

        >>> # `repeated` depends on parameters; we have to pass values for them to compute it
        >>> repeated.inspect(word="foo", repeats=3) # doctest: +SKIP
        'foo foo foo '

        >>> # turn `repeated` into a Function that takes those parameters
        >>> repeat = wf.Function.from_object(repeated)
        >>> repeat
        <descarteslabs.workflows.types.function.function.Function[{'word': Str, 'repeats': Int}, Str] object at 0x...>
        >>> repeat("foo", 3).inspect() # doctest: +SKIP
        'foo foo foo '
        >>> repeat("hello", 2).inspect() # doctest: +SKIP
        'hello hello '

        Parameters
        ----------
        obj: Proxytype
            A Workflows proxy object.

        Returns
        -------
        func: Function
            A `Function` equivalent to ``obj`` TODO
        """
        if any(p is obj for p in obj.params):
            raise ValueError(
                f"Cannot create a Function from a parameter object. This parameter {obj._name!r} "
                "is like an argument to a function---not the body of the function itself."
            )

        named_args = {p._name: getattr(p, "_proxytype", type(p)) for p in obj.params}
        # ^ if any of the params are widgets (likely), use their base Proxytype in the Function type signature:
        # a Function[Checkbox, Slider, ...] would be 1) weird and 2) not serializeable.
        concrete_function_type = cls[named_args, type(obj)]

        graft = client.function_graft(obj, *(p.graft for p in obj.params))
        # TODO we should probably store `obj.params` somewhere---that's valuable metadata maybe
        # to show the function as widgets, etc?
        return concrete_function_type._from_graft(graft)

    @classmethod
    def from_callable(cls, func, *arg_types, return_type=None):
        """
        Construct a Workflows Function from a Python callable.

        You must specify the types of arguments the function takes, either through type annotations
        (preferable) or directly in `from_callable`.

        If the function has type annotations, `from_callable` can be used as a decorator::

            @wf.Function.from_callable
            def my_function(x: wf.Int, y: wf.Image) -> wf.ImageCollection:
                ...

        Otherwise, the argument types must be passed to `from_callable`.

        Parameters
        ----------
        func: Python callable or Function
            The function to convert.

            A function is delayed by calling it once, passing in dummy Workflows objects and seeing
            what operations were applied in the value it returns.

            If ``func`` is already a Workflows `Function`, its argument types and return type
            must be compatible with ``arg_types`` and ``return_type``, if they're given. Specifically:

            * If ``arg_types`` are given, ``func`` must take that number of arguments, and each argument type
              must be a superclass of the corresponding one in ``arg_types``. Otherwise, it can take any arguments.
            * If ``return_type`` is give, ``func`` must return a subclass of ``return_type``. Otherwise, it can
              return any type.
        *arg_types: Proxytype, optional
            For each parameter of ``func``, the type that it should accept.
            The number of argument types given much match the number of arguments ``func`` actually accepts.
            If not given, ``func`` must have type annotations for all of its arguments.
            If ``func`` has type annotations, but ``arg_types`` are also given explicitly, then the annotations
            are ignored.
        return_type: Proxytype, optional
            The type the function should return. If not given, and there is no return type annotation,
            the return type will be inferred from what the function actually returns when called.

        Returns
        -------
        ~descarteslabs.function.Function

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> def string_pow(base: wf.Str, exp: wf.Float) -> wf.Float:
        ...     return wf.Float(base) ** exp
        >>> wf_string_pow = wf.Function.from_callable(string_pow)  # types inferred from annotations
        >>> print(wf_string_pow)
        <descarteslabs.workflows.types.function.function.Function[{'base': Str, 'exp': Float}, Float] object at 0x...>
        >>> wf_string_pow("2", 2.0).inspect()  # doctest: +SKIP
        4.0

        >>> # or, passing Str and Float as the argument types explicitly:
        >>> def string_pow(base, exp):
        ...     return wf.Float(base) ** exp
        >>> wf_pow = Function.from_callable(string_pow, wf.Str, wf.Float, return_type=wf.Float)
        >>> wf_pow
        <descarteslabs.workflows.types.function.function.Function[{'base': Str, 'exp': Float}, Float] object at 0x...>
        >>> wf_pow("3", 2.0).inspect()  # doctest: +SKIP
        9.0
        """
        if cls._type_params is not None:
            raise TypeError(
                f"Cannot call `from_callable` on a concrete Function type ({cls.__name__}). "
                "Instead, use the constructor directly, like:\n"
                f"{cls.__name__}({func!r})\n\n"
                "You should only use `from_callable` on the base Function class (`wf.Function.from_callable`).\n"
                "If you want to restrict both the arguments and the return type that's allowed, use __init__ on a "
                "concrete Function type as shown above (it's just easier).\n"
                "If you want to restrict only one (or none) of those, then use `wf.Function.from_callable` and "
                "pass `arg_types` and `return_type` accordingly."
            )

        if isinstance(func, Function):
            if return_type is None:
                return_type = func.return_type
            if len(arg_types) == 0:
                arg_types = func.all_arg_types

            expected_cls = Function[arg_types + ({}, return_type)]

            if isinstance(func, expected_cls):
                return func

            raise TypeError(
                f"Expected a {expected_cls.__name__}. "
                f"Got a {type(func).__name__}.\n"
                "Their signatures are incompatible:\n"
                f"need: {expected_cls.__signature__}\n"
                f"got:  {func.__signature__}"
            )

        arg_names = tuple(signature(func).parameters)

        if len(arg_types) == 0:
            arg_types, return_type = arg_types_from_annotations(func)

        for arg_type, name in zip(arg_types, arg_names):
            if not issubclass(arg_type, Proxytype):
                raise TypeError(
                    "For parameter {!r} to function {!r}: type annotation must be a Proxytype, "
                    "not {}".format(name, func.__name__, arg_type)
                )

        if return_type is not None and not issubclass(return_type, Proxytype):
            raise TypeError(
                "For return type of function {!r}: "
                "type annotation must be a Proxytype, not {}".format(
                    func.__name__, return_type
                )
            )

        result = cls._delay(func, return_type, *arg_types)
        result_type = type(result)

        concrete_type = cls[dict(zip(arg_names, arg_types)), result_type]
        instance = result._cast(concrete_type)

        if func.__doc__:
            instance.__doc__ = func.__doc__

        return instance

    # NOTE(gabe): this method will inherently fail to describe functions that return literals,
    # since if you just return a literal value from `func` that didn't interact with
    # the dummy parameters at all, there's no way for us to trace the dependency on the params
    # and therefore generate a proper function graft. A context manager system that logs
    # nested scope might be a better way to go for that reason, plus common subexpressions.
    @staticmethod
    def _delay(func, returns, *expected_arg_types, **expected_kwarg_types):
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
            Python callable. Must only take required positional arguments.
        returns: Proxytype or None
            The return value of the function is promoted to this type.
            If promotion fails, raises an error.
            If None, no promotion is attempted, and whatever ``func`` returned
            is returned from ``_delay``.
        *expected_arg_types: Proxytype
            Types of each positional argument to ``func``.
            An instance of each is passed into ``func``.
        *expected_kwarg_types: Proxytype
            Types of each named argument to ``func``.
            An instance of each is passed into ``func``.

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

        for name, param in func_signature.parameters.items():
            if param.kind not in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
                raise TypeError(
                    "Workflows Functions only support positional arguments. "
                    f"Parameter kind {param.kind!s}, used for {param} in the function "
                    f"{func.__name__}, is unsupported."
                )

            if param.default is not param.empty:
                raise TypeError(
                    f"Parameter {param} has a default value. Optional parameters "
                    "(parameters with default values) are not supported in Workflows functions."
                )

        try:
            # this will raise TypeError if the expected arguments
            # aren't compatible with the signature for `func`
            bound_expected_args = func_signature.bind(
                *expected_arg_types, **expected_kwarg_types
            ).arguments
        except TypeError as e:
            expected_sig = _make_signature(
                expected_arg_types, expected_kwarg_types, returns
            )
            raise TypeError(
                "Your function takes the wrong parameters.\n"
                f"Expected signature:        {expected_sig}\n"
                f"Your function's signature: {func_signature}.\n\n"
                f"When trying to call your function with those {len(expected_arg_types) + len(expected_kwarg_types)} "
                f"expected arguments, the specific error was: {e}"
            ) from None

        args = {
            name: identifier(name, type_) for name, type_ in bound_expected_args.items()
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
            ),
            params=result.params,
        )


def arg_types_from_annotations(func):
    """
    Gets argument types from type anotations
    """
    sig = signature(func)
    hints = typing.get_type_hints(func)

    argtypes = []
    for name, param in sig.parameters.items():
        try:
            argtype = hints[name]
        except KeyError:
            raise TypeError(
                "No type annotation given for parameter {!r} to function {!r}".format(
                    name, func.__name__
                )
            )

        argtypes.append(argtype)

    returns = hints.get("return", None)
    return tuple(argtypes), returns
