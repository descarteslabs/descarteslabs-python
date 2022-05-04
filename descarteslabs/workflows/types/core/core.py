from ....common.graft import client, syntax
from .exceptions import ProxyTypeError


class Castable(object):
    """
    Mixin to support casting and instantiation from grafts as separate constructors from ``__init__``

    Provides methods to construct delayed objects from grafts, and to propagate along
    any graft parameters they depend on.
    """

    @classmethod
    def _from_graft(cls, graft, params=()):
        """
        Create an instance of this class with the given graft dict and parameters, circumventing ``__init__``

        To use safely, this class must function correctly if its ``__init__`` method is not called.

        This is used to let your class present a friendly user-facing constructor,
        but still support copy-constuction or casting when necessary.
        """
        assert syntax.is_graft(
            graft
        ), "Attempted to instantiate {} from the non-graft-like object {!r}".format(
            cls.__name__, graft
        )
        assert isinstance(params, tuple), f"params must be a tuple, not {params!r}"

        # create a new, empty ``cls`` object, circumventing its __init__ method.
        new = cls.__new__(cls)
        new.graft = graft
        new.params = params
        return new

    @classmethod
    def _as_param(cls, name: str):
        "Construct a new instance of `cls` as a parameter referencing `name`."
        new = cls._from_graft(client.keyref_graft(name))
        new.params = (new,)
        # ^ NOTE: this is all that makes something a parameter: its params list contains itself.
        # that way, all other objects that interact with it will propagate that parameter forward.
        new._name = name
        return new

    @classmethod
    def _from_apply(cls, function, *args, **kwargs):
        """
        Construct a new instance of `cls` from a graft function application.

        Like ``cls._from_graft(client.apply_graft(function, *args, **kwargs))``,
        with parameter merging of the inputs.
        """
        return cls._from_graft(
            client.apply_graft(function, *args, **kwargs),
            params=merge_params(function, *args, *kwargs.values()),
        )

    def _cast(self, cls):
        "Copy of ``self`` as ``cls``. ``cls.__init__`` will not be called."
        return cls._from_graft(self.graft, self.params)


def merge_params(*args: Castable) -> tuple:
    "Returns merged and deduplicated parameters from the args"
    params = {}
    for arg in args:
        for p in getattr(arg, "params", ()):
            current = params.setdefault(p._name, p)
            if current is not p:
                raise ValueError(
                    f"Arguments combine two different parameter objects with the same names. "
                    f"A parameter named {p._name!r} ({p!r}) is used by the argument {arg!r}, "
                    f"but a prior argument used a different parameter object named {p._name!r}: "
                    f"{current!r}.\n\n"
                    f"If these parameters {p._name!r} mean different things, then give them different names. "
                    f"Otherwise, use the same parameter object for both."
                )
    return tuple(params.values())


class Proxytype(Castable):
    "Proxytype abstract base class"

    @classmethod
    def _promote(cls, obj):
        "Returns `obj` as type `cls`, or raises `ProxyTypeError` if promotion is not possible."
        if isinstance(obj, cls):
            return obj
        try:
            # Promote Any via casting. To avoid circular imports, we duck-type this
            # and consider `.cast` to be an interface; if an object has a `.cast` method
            # we might as well try it.
            return obj.cast(cls)
        except Exception:
            raise ProxyTypeError(
                "Cannot promote {} to {}".format(obj, cls.__name__)
            ) from None

    def __bool__(self):
        # Ensure Proxytypes can't be used in conditionals;
        # Python default would always resolve to True.
        raise TypeError(
            "Truth value of Proxytype {} objects is not supported".format(
                type(self).__name__
            )
        )

    def __contains__(self, _):
        if hasattr(self, "contains"):
            raise TypeError(
                (
                    "Please use {}.contains(other). Python requires a bool to be returned "
                    "from __contains__ and this value cannot be known for proxy types."
                ).format(type(self).__name__)
            )
        else:
            raise TypeError(
                "object of type {} does not support `in`.".format(type(self).__name__)
            )

    def __len__(self):
        if hasattr(self, "length"):
            raise TypeError(
                (
                    "Please use {}.length(). Python requires an int to be returned "
                    "from __len__ and this value cannot be known for proxy types."
                ).format(type(self).__name__)
            )
        else:
            raise TypeError(
                "object of type {} has no len()".format(type(self).__name__)
            )

    def __iter__(self):
        if hasattr(self, "map"):
            raise TypeError(
                (
                    "Proxy {0} is not iterable. Consider using {0}.map(...) instead."
                ).format(type(self).__name__)
            )
        else:
            raise TypeError(
                "object of type {} is not iterable.".format(type(self).__name__)
            )

    # NOTE(gabe): if you're looking for the `compute` and `persist` helper methods,
    # they're actually monkey-patched into here in the top-level
    # ``//descarteslabs/workflows/__init__.py`` to avoid circular dependencies
    # that are deemed a greater evil than monkey-patching.


class GenericProxytypeMetaclass(type):
    """
    Override ``isinstance`` and ``issubclass`` to make them covariant
    for `GenericProxytype`s and their concrete subclasses.

    Types can modify this behavior by implementing the classmethod
    ``_issubclass(cls, other: type) -> bool``.
    This is called on the possible supertype (second argument to ``issubclass``).
    If both types implement ``_issubclass``, only the possible supertype's is called.
    ``_issubclass`` is called iff both types inherit from the same generic type.
    For example, ``Foo._issubclass`` is not called in either ``issubclass(Bar[Int], Foo[Int])``
    or ``issubclass(int, Foo[Int])``, because the other type in question isn't also a ``Foo``.

    If ``_issubclass`` is not defined on the possible supertype,
    then the default behavior applies. Formally:

    ``issubclass(typA, typB) is True`` iff the generic type of ``typA``
    is a subclass of the generic type of ``typB``, and all the type parameters of ``typA``
    are subclasses of---or equal to, in the case of primitive values---the equivalent
    type parameters of ``typB``

    This ensures that:

    >>> from descarteslabs.workflows import List, Int
    >>> class MyIntSubclass(Int):
    ...     pass
    >>> class MyListSubclass(List):
    ...     pass
    >>> issubclass(List[MyIntSubclass], List[Int])
    True
    >>> issubclass(MyListSubclass[Int], List[Int])
    True
    >>> issubclass(MyListSubclass[MyIntSubclass], List[Int])
    True
    >>> issubclass(MyListSubclass[MyIntSubclass], List)
    True
    >>> issubclass(List[Int], List[MyIntSubclass])
    False

    The metaclass also adds an empty `_concrete_subtypes` dict to every new `GenericProxytype`,
    which is a registry of concrete subtypes of each generic type,
    so that if we've already created a type for the given parameters,
    we return that type object instead of making a duplicate.

    This ensure that, for example, ``List[Int]`` is always the same object,
    so comparing ``typA is typB`` is a safe way to determine iff they're exactly the same type.

    Horrifyingly, it also implements ``__getitem__`` to emulate ``__class_getitem__`` in Python < 3.7,
    so ``List[Str]`` works in both versions.
    """

    def __instancecheck__(self, obj):
        "Covariantly check whether ``isinstance(obj, type(self))``"
        return self.__subclasscheck__(type(obj))

    def __subclasscheck__(mycls, othercls):
        "Covariantly check whether ``issubclass(othercls, mycls)``"
        if not isinstance(othercls, type):
            raise TypeError("issubclass() arg 1 must be a class")

        # vanilla `issubclass`, to prevent infinite recursion into this `__subclasscheck__` method
        # (except the order of the args is reversed from `issubclass`, so call it "issuperclass" instead)
        issuperclass = type.__subclasscheck__

        # Get the generic (unparameterized) versions of both classes, e.g. List[Int] -> List
        # (`_generictype` is set on all concrete subtypes by `GenericProxytype.__class_getitem__`)
        try:
            my_generic_parent_type = mycls._generictype
        except AttributeError:
            assert issuperclass(GenericProxytype, mycls), (
                f"{mycls} is not a subclass of GenericProxytype. "
                "Do not use the GenericProxytypeMetaclass on it!"
            )
            # If `mycls` doesn't have a `_generictype` attr, it's already a generic
            my_generic_parent_type = mycls

        try:
            other_generic_parent_type = othercls._generictype
        except AttributeError:
            # NOTE(gabe): `other_generic_parent_type` might not actually be a GenericProxytype at all here,
            # but we'll catch that later in the final conditional
            other_generic_parent_type = othercls

        assert my_generic_parent_type._type_params is None, (
            f"Expected {my_generic_parent_type} to be generic, "
            f"but _type_params is not None: {my_generic_parent_type._type_params}"
        )
        assert getattr(other_generic_parent_type, "_type_params", None) is None, (
            f"Expected {other_generic_parent_type} to be generic, "
            f"but _type_params is not None: {other_generic_parent_type._type_params}"
        )

        # Check that the generic type of the other is a subclass of our generic type,
        # and either we're a generic ourselves (in which case other passes whether generic or concrete),
        # or the type parameters of other are all subclasses of our equivalent type parameters
        return issuperclass(my_generic_parent_type, other_generic_parent_type) and (
            mycls._type_params is None
            or mycls._issubclass(othercls)
            # NOTE: the default `_issubclass` is defined on `GenericProxytype`,
            # but classes can override if they wish.
        )

    def __init__(cls, name, bases, dct, **kwargs):
        # Ensure every subclass gets its own `_concrete_subtypes` dict,
        # which it will use to register parameterized (concrete) subtypes
        # (so calling `List[Int]` twice will return the same object both times)

        # QUESTION(gabe): does it matter that this happens on the concrete subtypes themselves?
        cls._concrete_subtypes = {}
        super(GenericProxytypeMetaclass, cls).__init__(name, bases, dct, **kwargs)

    def __getitem__(cls, idx):
        "Emulate __class_getitem__ for Python < 3.7"
        try:
            return cls.__class_getitem__(idx)
        except AttributeError:
            raise TypeError("{!r} object is not subscriptable".format(cls.__name__))


def type_params_issubclass(type_params: tuple, super_type_params: tuple) -> bool:
    """
    Whether each element in `type_params` is a subclass of the equivalent element in `super_type_params`.

    Recursively descends tuples and dicts in the type params to determine if
    `type_params` covariantly describes a subclass of `super_type_params`.
    """
    if type(type_params) is not type(super_type_params):
        return False

    if isinstance(type_params, PRIMITIVES):
        return type_params == super_type_params
    elif isinstance(type_params, tuple):
        return (
            isinstance(super_type_params, tuple)
            and len(type_params) == len(super_type_params)
            and all(
                type_params_issubclass(cls, super_cls)
                for cls, super_cls in zip(type_params, super_type_params)
            )
        )
    elif isinstance(type_params, dict):
        return (
            isinstance(super_type_params, dict)
            and len(type_params) == len(super_type_params)
            and all(
                # NOTE: dict order matters!
                type_params_issubclass(k, super_k)
                and type_params_issubclass(v, super_v)
                for (k, v), (super_k, super_v) in zip(
                    type_params.items(), super_type_params.items()
                )
            )
        )
    else:
        return issubclass(type_params, super_type_params)


class GenericProxytype(Proxytype, metaclass=GenericProxytypeMetaclass):
    """
    Abstract base class for generic Proxytypes; i.e. abstract types that can be parameterized with other types.

    You can't instantiate this class directly; instead, use a built-in container type (like List, Tuple, etc.)
    or create your own by subclassing `GenericProxytype`.

    Type Parameters
    ---------------

    Type parameters can be given as 1 or more:

        * Subclasses of `Proxytype`
        * Dicts with string keys, and values that are subclasses of `Proxytype`

    For example, ``List[Int]``, ``Tuple[Str, Float]``, ``Function[{'a': Bool, 'b': Int}, Int]``
    are all valid parameterizations. ``Tuple[[Str, Float], Bool]`` and ``List["foo"]`` are not.

    Subclassing Instructions
    ------------------------

    You need to:

        * Write an appropriate ``__init__`` method.

          This should include a check like this:
          ```
          if self._type_params is None:
              raise TypeError(
                  f"Cannot instantiate a generic {type(self).__name__}; the item type must be specified "
                  f"like {type(self).__name__}[<example of using type params>].
              )
          ```

          You can also do this with:
          ```
          try:
              super().__init__()
          except TypeError:
              raise TypeError(
                  f"Cannot instantiate a generic {type(self).__name__}; the item type must be specified "
                  f"like {type(self).__name__}[<example of using type params>].
              ) from None
          ```

          If your Proxytype is representing a Python type, ``__init__`` should probably resemble
          the behavior of the Python type, and/or be able to take an instance of the Python type
          and represent it as the proxy type.

          After ``__init__`` is called, the object should conform to the Delayed interface;
          i.e. ``self.graft`` should be set. (Not mandatory, but typically what makes sense.)

        * If the default ``_promote`` classmethod doesn't work for you,
          which just calls ``__init__`` with whatever it's passed, then override it
          such that ``cls._promote(x)`` will return an instance of ``cls`` representing ``x``,
          or raise `ProxyTypeError` if promotion is not possible.

        * Write all the other methods to make your class useful (``__getattr__``, ``__getitem__``,
          ``calculate_secret_algorithm``, etc.). These will likely make heavy use of a pattern like
          ``return self._type_params[0]._from_apply(funcname, obj=self, ...)``.

    When creating a generic Proxytype, remember that in all the instance methods you write,
    ``self`` will be a _concrete_ subclass of your generic type, and ``self._type_params``
    will be a tuple of the type parameters for that concrete type. For example, though you
    might write ``class List(GenericProxytype)``, in ``__getitem__(self, idx)``, ``self``
    is a ``List[Float]``, or a ``List[List[Int]]``, etc. (and ``self._type_params`` would be
    ``(Float,)`` or ``(List[Int],)`` in those examples).

    `GenericProxytype` provides an implementation of `__class_getitem__` for you: the machinery that makes
    ``List[Int]`` work, for example. This implementation:

        * Checks that you're actually parameterizing a generic, not a concrete type
          (``List[Int][Str]`` will raise a `TypeError`)
        * Validates that all parameters are Proxytypes, or dicts containing Proxytypes as values.
          Note that the number or type of type parameters is _not_ checked.
        * Dynamically constructs a new concrete subclass of your generic type, where ``_type_params`` is set to
          the tuple of whatever was passed into ``__class_getitem__`` and ``_generictype`` is a reference
          to the parent generic type
        * Caches all concrete subclasses, so ``List[Int]`` will always return the same object,
          rather than constructing a new type on the fly every time

    By implementing ``__class_getitem_hook__``, you can customize the creation of concrete subclass objects.
    ``__class_getitem_hook__`` is called with ``name, bases, dct, type_params``
    (same as the arguments to ``type()``, plus ``type_params`` for convenience), and should return
    ``name, bases, dct``. These returned values are what are ultimately passed into ``type()``
    to dynamically construct the concrete subclass. ``__class_getitem_hook__`` must not modify
    ``dct["_type_params"]``, because that could invalidate the ``_concrete_subtypes`` registry.
    ``__class_getitem_hook__`` should be a `staticmethod`.

    The typical use-case for ``__class_getitem_hook__`` is adding methods or fields to ``dct`` based
    on ``type_params``, such as `Struct` does to add property getter functions for each of its fields.

    Since `GenericProxytype` uses the `GenericProxytypeMetaclass`, by inheriting from it,
    the Python ``isinstance`` and ``issubclass`` methods will also behave covariantly for your
    custom generic type: ``issubclass(MyType[MyIntSubclass], `MyType[Int])`` will be True.

    To customize the behavior of ``isinstance`` and ``issubclass`` checking, override the
    ``_issubclass`` classmethod. By default, it checks whether corresponding elements
    in the two class's type parameters are all subclasses, or equal. See `GenericProxytypeMetaclass`
    for more.

    By implementing ``_validate_params``, you can customize the valid type parameters of a class.
    ``_validate_params`` should take a tuple of type parameters and apply appropriate validation
    to each (in most cases you will at least want to validate that each type parameter is a subclass of Proxytype).
    """

    _type_params = None

    @classmethod
    def __class_getitem__(cls, type_params):
        if cls._type_params is not None:
            raise TypeError(
                "{} type is not subscriptable "
                "(it's no longer generic and already has type parameters applied)".format(
                    cls.__name__
                )
            )

        type_params = validate_typespec(type_params)

        # Look up these parameters in the _concrete_subtypes registry. If we've already created a
        # type for these parameters, return that object instead of making a duplicate.
        # It's a helpful invariant elsewhere to know that iff `clsA is clsB` (they're the same object),
        # then `clsA` and `clsB` are the same generic class, parameterized with the same types,
        # and can be safely considered equivalent.
        # (`cls._concrete_subtypes` was added by the GenericProxytypeMetaclass, and is an empty dict.)
        hashable_type_params = tuple(
            tuple(x.items()) if isinstance(x, dict) else x for x in type_params
        )
        try:
            return cls._concrete_subtypes[hashable_type_params]
        except KeyError:
            if hasattr(cls, "_validate_params"):
                cls._validate_params(type_params)

            param_names = (
                # Default dict formatting would print values like `<class 'descarteslabs.common.proxytypes.Int'>`,
                # so we do our own dict formatting using `cls.__name__` for read/copy-ability
                "{{{}}}".format(
                    ", ".join(
                        "{!r}: {}".format(
                            k,
                            param.__name__ if isinstance(param, type) else repr(param),
                        )
                        for k, param in type_param.items()
                    )
                )
                if isinstance(type_param, dict)
                else type_param.__name__
                if isinstance(type_param, type)
                else repr(type_param)
                for type_param in type_params
            )

            parameterized_name = "{}[{}]".format(cls.__name__, ", ".join(param_names))
            bases = (cls,)
            dct = {
                "_type_params": type_params,
                "_generictype": cls,
                "__module__": cls.__module__,
                # ^ so repr shows the concrete class as coming from its own module, not here
            }

            if hasattr(cls, "__class_getitem_hook__"):
                # allow subclasses to customize their parameterization
                # NOTE: subclasses MUST NOT modify `dct["_type_params"]`,
                # because that could invalidate the `_concrete_subtypes` registry.
                parameterized_name, bases, dct = cls.__class_getitem_hook__(
                    parameterized_name, bases, dct, type_params
                )

            # Dynamically construct a subclass of the generic type,
            # with `_type_params` set (making it a concrete type),
            # and a reference to this parent generic type (used for `issubclass` checking by the metaclass).
            ConcreteType = type(parameterized_name, bases, dct)

            cls._concrete_subtypes[hashable_type_params] = ConcreteType
            return ConcreteType

    @classmethod
    def _issubclass(cls, other: type) -> bool:
        """
        Whether ``other`` is a subclass of the current class.

        Overriding this method allows customization of ``isinstance`` and ``issubclass`` checks.

        Note that ``_issubclass`` is only called when both classes are the same
        *generic* type; i.e. inside this method for ``List[Int]``, you can assume that
        ``other`` is also a subclass of ``List``.
        """
        return type_params_issubclass(other._type_params, cls._type_params)

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        try:
            # Promote Any via casting. To avoid circular imports, we duck-type this
            # and consider `.cast` to be an interface; if an object has a `.cast` method
            # we might as well try it.
            return obj.cast(cls)
        except Exception:
            pass

        try:
            return cls(obj)
        except Exception as e:
            raise ProxyTypeError(e)

    @classmethod
    def _from_graft(cls, graft, params=()):
        """
        Create an instance of this class with the given graft dict and params, circumventing ``__init__``

        To use safely, this class must function correctly if its ``__init__`` method is not called.

        This is used to let your class present a friendly user-facing constructor,
        but still support copy-constuction or casting when necessary.
        """
        assert not is_generic(cls), "Cannot instantiate a generic {}".format(
            cls.__name__
        )
        return super(GenericProxytype, cls)._from_graft(graft, params=params)

    def __init__(self):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic {0}; the type parameters must be provided "
                "(like `{0}[...]`)".format(type(self).__name__)
            )


def is_generic(type_):
    """
    Returns true if `type_` is generic, meaning it is a subclass of
    `GenericProxytype` and has no specified type parameters, or any of its type parameters are are also generic
    """
    type_parameters = getattr(type_, "_type_params", None)

    if type_parameters is not None:
        return any(map(is_generic, type_parameters))

    if isinstance(type_, dict):
        # in this case `type_` is a 'complex type', `dict[Type:Type]`
        complex_type = type_
        return any(map(is_generic, complex_type.keys())) and any(
            map(is_generic, complex_type.values())
        )

    if isinstance(type_, PRIMITIVES):
        return False
    return issubclass(type_, GenericProxytype)


PRIMITIVES = (int, float, bool, str)


def validate_typespec(type_params):
    "Ensures that type parameters are of a valid form (Proxytype, Python primitive, tuples or dicts of these)"
    if not isinstance(type_params, tuple):
        type_params = (type_params,)
    for i, type_param in enumerate(type_params):
        if isinstance(type_param, dict):
            for key, param_cls in type_param.items():
                validate_typespec(key)
                validate_typespec(param_cls)
        elif isinstance(type_param, PRIMITIVES):
            pass
        else:
            # we try-except here so we can throw a better
            # error when type_param is a Proxytype instance
            # rather than the issubclass error
            try:
                assert issubclass(type_param, Proxytype)
            except (TypeError, AssertionError):
                raise TypeError(
                    "Type parameters must be Proxytypes, Python primitive values, "
                    "or tuples or dicts of those, but got {!r}".format(type_param)
                )
    return type_params


def assert_is_proxytype(type_, error_message=None):
    try:
        assert issubclass(type_, Proxytype)
    except (AssertionError, TypeError):
        if error_message is None:
            error_message = "Expected a Proxytype type object, but got {!r}".format(
                type_
            )
        raise TypeError(error_message) from None
