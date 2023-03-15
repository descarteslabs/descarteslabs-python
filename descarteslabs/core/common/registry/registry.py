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
