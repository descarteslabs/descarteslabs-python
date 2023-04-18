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

from typing import TypeVar, Optional, Mapping, Callable, Tuple

K = TypeVar("K")
V = TypeVar("V")


def registry(
    mapping: Optional[Mapping[K, V]] = None, error_on_overwrite: bool = True
) -> Tuple[Mapping[K, V], Callable[[K], Callable[[V], V]]]:
    """
    Construct a registry and a decorator for registering functions or classes into it.

    Parameters
    ----------
    mapping: mapping, optional, default None
        The mapping to use as a registry. If None, creates an empty dict for you.
    error_on_overwrite: bool, default True
        Whether to raise a ValueError when attempting to register a new value
        for a key that's already been registered.

    Returns
    -------
    mapping: mapping
        The mapping things will be registered to
    register: function
        Decorator that takes 1 argument: the key under which to register the decorated thing

    Example
    -------
    >>> REGISTRY, register = registry()
    >>> @register("foo")
    ... def foo_func():
    ...     pass
    >>> @register("bar")
    ... def bar_func():
    ...     pass
    >>> print(REGISTRY)
    {'foo': <function foo_func at 0x..., 'bar': <function bar_func at 0x...}
    """
    if mapping is None:
        mapping = {}

    def register(key: K) -> Callable[[V], V]:
        def deco(obj: V) -> V:
            existing = mapping.setdefault(key, obj)
            if error_on_overwrite and existing is not obj:
                raise ValueError(
                    "Attempted to register {!r} to key {!r}, "
                    "which is already registered to {!r}".format(obj, key, existing)
                )
            return obj

        return deco

    return mapping, register
