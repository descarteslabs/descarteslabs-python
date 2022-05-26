import functools
import inspect
import types

from collections import abc

from ....third_party import boltons

from .exceptions import ProxyTypeError


def _promote(obj, to_classes, argument_id, name):
    if isinstance(obj, to_classes):
        return obj
    else:
        if not isinstance(to_classes, (tuple, list)):
            to_classes = (to_classes,)

        errors = []
        for to_cls in to_classes:
            try:
                return to_cls._promote(obj)
            except ProxyTypeError as e:
                errors.append(e)

        to_classes_str = ", ".join(cls.__name__ for cls in to_classes)

        msg = (
            "Argument {arg_id!r} to function {name}: "
            "expected {to_classes_str} or an object promotable to {that_those}, "
            "but got {type_obj}: {obj!r}".format(
                arg_id=argument_id,
                name=name,
                to_classes_str=to_classes_str,
                that_those="that" if len(to_classes) == 1 else "those",
                type_obj=type(obj).__name__,
                obj=obj,
            )
        )

        error_msgs = [
            "{}: {}".format(to_cls.__name__, error.args[0])
            for to_cls, error in zip(to_classes, errors)
        ]

        full_msg = "{}\n\nWhile promoting, these errors occured:\n\n{}".format(
            msg, "\n\n".join(error_msgs)
        )

        raise TypeError(full_msg)


# inspired by https://github.com/mrocklin/multipledispatch/blob/master/multipledispatch/core.py#L73-L84
# NOTE(gabe): `inspect.ismethod` doesn't work here, because while within the decorator,
# methods are not yet bound to their classes---so they just look like any other functions.
def _is_method(func):
    if hasattr(inspect, "signature"):
        signature = inspect.signature(func)
        return next(iter(signature.parameters)) in ("self", "cls")
    else:
        spec = inspect.getargspec(func)
        return spec and spec.args and spec.args[0] in ("self", "cls")


def _requires_self(func):
    # NOTE: we cannot use callable(to_classes); many things are callable
    if isinstance(func, types.FunctionType):
        sig = inspect.signature(func)
        return len(sig.parameters) > 0
    else:
        return False


def _resolve_lambdas(to_classes):
    # NOTE: we cannot use callable(to_classes); many things are callable
    if isinstance(to_classes, types.FunctionType):
        return to_classes()
    if isinstance(to_classes, abc.Mapping):
        return {k: _resolve_lambdas(v) for k, v in to_classes.items()}
    elif isinstance(to_classes, abc.Sequence):
        return tuple(_resolve_lambdas(item) for item in to_classes)
    else:
        return to_classes


def typecheck_promote(*expected_arg_types, **expected_kwarg_types):
    """
    Decorator to promote a function's arguments to specified Proxytypes.

    If promotion fails for an argument, a TypeError is raised, unless
    the ``_reflect`` kwarg is set in which case NotImplemented is returned.
    This indicates to the Python interpreter that, when dealing with
    binary ops, the reversed version of the function should be attempted
    before raising a TypeError.

    Expected types can be given as:

    * Proxytypes (``Dict[Str, Float]``)
    * Functions that evaluate to Proxytypes
      (``lambda: Int`` or ``lambda self: self._type_params[0]``)

      This is useful when writing a class and wanting to typecheck
      for instances of that class. Using the class's name directly
      in the decorator would be a ``NameError``; this way, you can
      defer the lookup with a lambda.

      When decorating methods, the function can take one argument,
      in which case it's passed the ``self`` or ``cls`` argument
      that the decorated method recieves. This is meant for generic
      Proxytypes: you can typecheck arguments based on the ``_type_params``
      of a concrete subtype. For example, a Dict could do:

        @typecheck_promote(lambda self: self._type_params[0])
        def __getitem__(self, idx):
            return self._type_params[1]._from_apply("getitem", self, idx)

      to typecheck that arguments to __getitem__ are of the correct
      key type.
    * Lists or tuples of the two above, to represent multiple options
      for a parameter (``[Int, Float]``)

      It'll attempt to promote the argument to each type in order,
      using the first that succeeds.

    When used on instance methods or classmethods, promotion of
    the ``self`` or ``cls`` argument is skipped.
    Note that on classmethods, the `typecheck_promote` decorator
    must go _before_ (below) the ``@classmethod`` decorator.

    Note that default values will _also_ be promoted,
    so if the default value for an argument is incompatible with
    its expected type, users will encounter a rather confusing error.

    Example
    -------
    >>> from descarteslabs.workflows import List, Str, Int, Float
    >>> from descarteslabs.workflows.types import typecheck_promote
    >>> @typecheck_promote(Int, List[Str], optional=Float)
    ... def my_function(an_int, a_list_of_str, optional=None):
    ...     assert isinstance(an_int, Int)
    ...     assert isinstance(a_list_of_str, List[Str])
    ...     assert isinstance(optional, Float)

    >>> my_function(0, ['a', 'b'], optional=2.2)
    ... # the arguments are automatically promoted from Python types,
    ... # so inside `my_function`, they're all Proxytypes
    >>> my_function('not_an_int', ['a', 'b'])
    Traceback (most recent call last):
        ...
    TypeError: Argument 'an_int' to function my_function(): \
    expected Int or an object promotable to that, but got str: 'not_an_int'
        ...
    """
    # assert all(isinstance(arg_type, Proxytype) for arg_type in expected_arg_types)
    # assert all(isinstance(arg_type, Proxytype) for arg_type in expected_kwarg_types.values())

    # NOTE(gabe): On Passing Expected Types As Lambdas
    # When defining a class, you might want to typecheck that methods recieve instances of that class.
    # However, at the scope level of the decorator, the name of the class is not yet defined---
    # you can't juse ``@typecheck_promote(MyClass)``; it'll be a NameError.

    # To get around this, we let you pass a function to essentially delay looking up the name
    # until after the class is defined: ``@typecheck_promote(lambda: MyClass)``.

    # Internally, we can't safely evaluate that lambda anywhere except within the method we're wrapping.
    # That's annoying, because we'd rather not re-do that every time you call the typechecked method.

    # So, we use a dummy list `have_resolved_lambdas` as a flag (if empty, we haven't done it;
    # if not empty, we have), and the first time the method is actually called, we _mutate_
    # the dict `bound_expected_args`, replacing params that were functions
    # with their returned values. Once that's done once, we never have to do it again.

    # Any lambdas that depend on `self` are recomputed every time the function is called,
    # because of course, `self` might change.

    have_resolved_lambdas = []  # empty means False

    reflect = expected_kwarg_types.pop("_reflect", False)

    def decorator(func):
        func_name = "{}()".format(func.__name__)
        func_signature = inspect.signature(func)
        is_method = _is_method(func)

        # insert placeholder for `self`/`cls`, so `bind` doesn't raise a TypeError
        expected_arg_types_with_self = (
            expected_arg_types if not is_method else (None,) + expected_arg_types
        )

        # this will raise TypeError if the expected arguments
        # aren't compatible with the signature for `func`
        bound_expected_args = func_signature.bind(
            *expected_arg_types_with_self, **expected_kwarg_types
        ).arguments

        if is_method:
            # split out lambdas that depend on `self`,
            # so we can recompute them on every function call,
            # unlike the primary `bound_expected_args` that can just be
            # computed once.
            self_lambdas = {
                name: func
                for name, func in bound_expected_args.items()
                if _requires_self(func)
            }

            for name in self_lambdas:
                del bound_expected_args[name]

        @boltons.funcutils.wraps(func)
        def typechecked_func(*args, **kwargs):
            if not have_resolved_lambdas:
                bound_expected_args.update(_resolve_lambdas(bound_expected_args))
                # ^ use `.update()` to mutate in-place
                have_resolved_lambdas.append(True)  # non-empty means True

            if is_method and len(self_lambdas) > 0:
                # there are argtypes that depend on `self`;
                # we need to recompute these on every function call
                self_reference = args[0]
                expected_types = dict(
                    bound_expected_args,
                    **{
                        name: func(self_reference)
                        for name, func in self_lambdas.items()
                    }
                )
            else:
                expected_types = bound_expected_args

            bound_args = func_signature.bind(*args, **kwargs)
            # ^ this will raise TypeError if incompatible arguments are given for `func`
            bound_args.apply_defaults()
            bound_args_dict = bound_args.arguments

            for name, argtype in expected_types.items():
                if argtype is None:
                    # it's a placeholder for `self`
                    continue
                value = bound_args_dict[name]
                try:
                    bound_args_dict[name] = _promote(value, argtype, name, func_name)
                except TypeError as e:
                    if reflect:
                        return NotImplemented
                    else:
                        # Re-raise TypeError
                        raise e from None

            promoted_args = bound_args.args
            promoted_kwargs = bound_args.kwargs
            return func(*promoted_args, **promoted_kwargs)

        # maybe this will be useful later
        typechecked_func._signature = (expected_arg_types, expected_kwarg_types)
        return typechecked_func

    return decorator


def allow_reflect(func):
    @functools.wraps(func)
    def wrapped(*args):
        try:
            return func(*args)
        except ProxyTypeError:
            return NotImplemented

    return wrapped
