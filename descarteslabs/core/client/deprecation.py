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

from functools import wraps
import inspect
import warnings


SUPPRESS_DEPRECATION_WARNINGS = "_suppress_deprecation_warnings"


def check_deprecated_kwargs(
    kwargs,
    renamed=None,
    required=None,
    deprecated=None,
    removed=None,
    stacklevel=2,
):
    """Support a deprecation cycle for function/method parameters

    - Changing renamed kwarg calls
    - Depcrecating but allowing kwarg calls
    - Removing kwarg calls
    - Specifying that some kwargs with defaults are required
    - Suppressing warnings in case calls are nested

    Parameters
    ----------
    kwargs : mapping
        The keyword parameters. If the keyword `_suppress_deprecation_warnings` is
        included, it must be an iterable of parameter names that are skipped in the
        `deprecated` and `removed` checks, and will be removed.
    renamed : mapping, optional
        A mapping {'old name': 'new name'}.  If `old_name` is found in the parameters,
        it will generate a warning and rename `old_name` to `new_name`. If `new_name`
        already exists, it will raise an `SyntaxError`.
    required : iterable, optional
        An iterable of parameter names that are required.  You only
        need to specify parameters that are kwargs with defaults, positional parameters
        are checked automatically.
    deprecated : iterable, optional
        An iterable of parameters names that are deprecated and will generate a warning
        without removing the parameter.
    removed : iterable, optional
        An iterable of parameters names that are deprecated and will generate a warning
        and also remove the parameter.
    stacklevel : int
        Which stack frame the warning message will use

    Raises
    ------
    SyntaxError
        A required kwarg is missing, or if both the
        old and new versions of the kwarg are specified by the called.
    TypeError
        More args are supplied than are defined in the function.

    Examples
    --------
    Renaming parameters:

        def f(a, b, kwarg=None):
            pass

        to rename ``b`` to ``c``

        @deprecate(renames={'b': 'c'})
        def f(a, c, kwarg=None):
            pass

    Making positional parameter optional:

        def f(a, b, c):
            pass

        to make ``b`` optional and ``c`` remain required

        @deprecate(required=['c']):
        def f(a, b=None, c=None):
            pass

    Warning about parameter use:

        def f(a, b=None, c=None):
            pass

        to warn if ``b`` is supplied but still pass it on

        @deprecate(deprecated=['b'])
        def f(a, b=None, c=None):
            pass

    Removing parameters:

        def f(a, b, c, kwarg=None):
            pass

        to remove ``b`` completely

        @deprecate(removed=['b'])
        def f(a, c, kwarg=None):
            pass

        calls like ``f('a', 'b', 'c', 'd')`` will raise a TypeError, so this is the
        last step in the deprecation cycle before removing the parameter or decorator
        completely.

    Suppressing duplicate warnings in a call hierarchy:

        It often arises that a parameter to be deprecated is passed through multiple
        function calls. Using a ``@deprecate(deprecated=['b'])`` or
        ``@deprecate(removed=['b'])`` on each of the
        functions will yield multiple warnings, when it is desired that only the
        top-most call should yield a warning. This can be achieved by passing the
        `_suppress_deprecation_warnings` parameter with the name of the params to ignore:

        def f(a, b=None, c=None):
            g(a, b=b, b=b)

        def g(a, b=None, c=None):
            pass

        To suppress a warning about ``b`` from ``g()`` when called by ``f()``

        @deprecate(deprecated=['b'])
        def f(a, b=None, c=None):
            g(a, b=b, c=c, _suppress_deprecation_warnings=['b'])

        @deprecate(deprecated=['b'])
        def g(a, b=None, c=None):
            pass
    """
    msgs = []
    suppress_deprecation_warnings = kwargs.pop(SUPPRESS_DEPRECATION_WARNINGS, None)

    if suppress_deprecation_warnings:
        for key in suppress_deprecation_warnings:
            if deprecated and key in deprecated:
                deprecated.remove(key)

            if removed and key in removed:
                removed.remove(key)

    if renamed:
        # Rename any parameters before checking the other cases
        for old, new in renamed.items():
            if old in kwargs:
                if new in kwargs:
                    msg = (
                        f"Parameter `{old}` has been renamed to `{new}`, and "
                        "will be removed in future versions. Do not specify both "
                        "parameters, and use only `{new}`."
                    )
                    raise SyntaxError(msg)
                else:
                    msgs.append(
                        f"Parameter `{old}` has been renamed to `{new}`, and "
                        "will be removed in future versions. Use "
                        f"`{new}` instead."
                    )
                    kwargs[new] = kwargs.pop(old)

    if required:
        for key in required:
            if key not in kwargs:
                raise SyntaxError(f"Missing required parameter {key}")

    if deprecated:
        for key in deprecated:
            if key in kwargs:
                msgs.append(
                    f"Parameter `{key}` has been deprecated and will be removed completely "
                    "in future versions."
                )

    if removed:
        for key in removed:
            if key in kwargs:
                msgs.append(
                    f"Parameter `{key}` has been deprecated and is no longer supported."
                )
                kwargs.pop(key)

    for msg in msgs:
        warnings.warn(msg, FutureWarning, stacklevel=stacklevel)


def deprecate_func(message=None):
    """
    This decorator emits a deprecation warning for a function with a custom
    message, if applicable.
    """

    def wrapper(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not kwargs.pop(SUPPRESS_DEPRECATION_WARNINGS, False):
                if message:
                    msg = message
                else:
                    msg = "{} has been deprecated and will be removed competely in a future version".format(
                        f.__name__
                    )
                warnings.warn(msg, FutureWarning, stacklevel=2)
            return f(*args, **kwargs)

        return wrapped

    return wrapper


def deprecate(
    renamed=None,
    required=None,
    deprecated=None,
    removed=None,
):
    """Decorator for a deprecation cycle as outlined in `check_deprecated_kwargs`.

    Don't use this decorator with varargs or kwargs.
    """

    def wrapper(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            func_spec = inspect.getfullargspec(f)
            func_args = func_spec.args
            signature = inspect.signature(f, follow_wrapped=False)
            signature_args = list(signature.parameters.keys())
            if (
                len(func_args) == len(signature_args) + 1
                and func_args[0] == "cls"
                and func_args[1:] == signature_args
            ):
                # it's a class method, lose the class arg
                func_args = signature_args
            # func_spec.args might be shorter than args due to removed
            # parameters, raise TypeError in these cases
            if len(args) > len(func_args):
                raise TypeError(
                    "{}() takes {} arguments "
                    "but {} were given.".format(f.__name__, len(args), len(func_args))
                )
            kwargs.update(dict(zip(func_args, args)))
            check_deprecated_kwargs(
                kwargs,
                renamed,
                required,
                deprecated,
                removed,
                stacklevel=3,
            )
            return f(**kwargs)

        return wrapped

    return wrapper
