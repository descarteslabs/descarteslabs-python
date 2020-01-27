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
import sys
import six

if sys.version_info.major >= 3:
    getargspec = inspect.getfullargspec
else:
    getargspec = inspect.getargspec


def check_deprecated_kwargs(kwargs, renames):
    """
    Warn for each key in ``kwargs`` that's been renamed.
    ``renames`` is a dict mapping {deprecated name : new name, or None if fully deprecated}
    """
    for field, renamed_to in six.iteritems(renames):
        if field in kwargs:
            if renamed_to is not None:
                msg = (
                    "The parameter `{old}` has been renamed to `{new}`."
                    "`{old}` will be removed in future versions, "
                    "please use `{new}` instead.".format(old=field, new=renamed_to)
                )
            else:
                msg = (
                    "The parameter `{}` has been deprecated "
                    "and will be removed in future versions.".format(field)
                )
            warnings.warn(msg, DeprecationWarning)


def deprecate(required=None, renames=None):
    """
    This decorator helps support a deprecation cycle for function parameters, and does
    a few separate things:

        Changing renamed kwarg calls
        Removing kwarg calls
        Specifying that some kwargs with defaults are required

    Don't use this decorator with varargs or kwargs.

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

        to make b optional and c remain required

        @deprecate(required=['c']):
        def f(a, b=None, c=None):
            pass

    Removing parameters:

        def f(a, b, c, kwarg=None):
            pass

        to remove b completely

        @deprecate(renames={'b': None})
        def f(a, c, kwarg=None):
            pass

        calls like ``f('a', 'b', 'c', 'd') will raise a TypeError, so this is the
        last step in the deprecation cycle before removing the parameter or decorator
        completely.


    Will raise a SyntaxError if a required kwarg is missing, or if both the
    old and new versions of the kwarg are specified by the called.  Raises
    TypeError if more args are supplied than are defined in the function.

    ``required`` is an iterable of parameter names that are required.  You only
    need to specify parameters that are kwargs with defaults, positional parameters
    are checked automatically.

    ``renames`` is a dict mapping {'old name': 'new name'}.  If 'new name' is
    None, it is assumed that the parameter is fully deprecated and will be discarded.
    """
    if renames is None:
        renames = {}

    if required is None:
        required = []

    def wrapper(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            func_spec = getargspec(f)
            # func_spec.args might be shorter than args due to removed
            # parameters, raise TypeError in these cases
            if len(args) > len(func_spec.args):
                raise TypeError(
                    "{}() takes {} arguments "
                    "but {} were given.".format(
                        f.__name__, len(args), len(func_spec.args)
                    )
                )
            kwargs.update(dict(zip(func_spec.args, args)))

            # rename any Parameters before checking required
            for old, new in six.iteritems(renames):
                if old not in kwargs:
                    continue

                if new is None:
                    msg = (
                        "Parameter `{old}` is deprecated and will be removed completely "
                        "in future versions."
                    ).format(old=old)
                    kwargs.pop(old)
                elif new not in kwargs:
                    kwargs[new] = kwargs.pop(old)
                    msg = (
                        "Parameter `{old}` has been renamed to `{new}`, and "
                        "will be removed in future versions. Use "
                        "`{new}` instead."
                    ).format(old=old, new=new)

                else:
                    msg = (
                        "Parameter `{old}` has been renamed to `{new}`, and "
                        "will be removed in future versions. Do not specify both "
                        "parameters, and use only `{new}`."
                    ).format(old=old, new=new)
                    raise SyntaxError(msg)

                warnings.warn(msg, DeprecationWarning)

            for key in required:
                if key not in kwargs:
                    raise SyntaxError("Missing required parameter {}".format(key))

            return f(**kwargs)

        return wrapped

    return wrapper
