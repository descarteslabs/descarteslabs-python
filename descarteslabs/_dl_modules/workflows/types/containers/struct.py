from ....common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError, GenericProxytype, assert_is_proxytype, merge_params
from ..primitives import NoneType, Any


def _dict_repr_to_constructor_syntax(string):
    """
    In string, convert literal dict reprs like ``{'x': 1}`` to constructors like ``dict(x=1)``.

    Only works for dicts without string values (which is fine for a typespec, where values are always Proxytypes)
    """
    return (
        string.replace("{", "dict(")
        .replace("}", ")")
        .replace(": ", "=")
        .replace("'", "")
    )


def _property_factory(field, field_cls, doc=None):
    def _getter(self):
        return self._attr(field)

    field_cls_name = field_cls.__name__
    if "{" in field_cls_name:
        # NOTE(gabe): Work around a bizarre `sphinx.ext.napoleon` bug,
        # which ignores inline markup (like ``) when searching for colons,
        # causing it to split the type and message parts at the first colon
        # in a dict's repr. Converting to `dict(x=1)`-like syntax seemed to be
        # the best way to keep unmangled docstrings for IPython while remaining
        # sphinx-compatible
        field_cls_name = _dict_repr_to_constructor_syntax(field_cls_name)
    if doc is not None:
        doc = "{}: {}".format(field_cls_name, doc)
    else:
        doc = field_cls_name

    _getter.__doc__ = doc
    _getter.__name__ = field
    return property(_getter)


@serializable()
class Struct(GenericProxytype):
    """
    ``Struct[{field_name: type, ...}]``: Proxy container with named fields of specific types,
    meant as a helper base class.

    Can be instantiated from kwargs.

    In general, Struct is used as an internal base class to help create proxy objects,
    and is not meant to be worked with directly.

    Notes
    -----
    Adding certain fields to subclasses of concrete Structs changes their behavior:

    * ``_doc`` (dict of str): Docstrings for struct fields.
      The keys must match keys in the typespec (though not all fields must be documented).
      Resulting docstrings are formatted like ``"{}: {}".format(return_type_name, given_docstring)``.
      If no docstring is given, the field getter will just be documented with its return type.

    * ``_constructor`` (str): Function name to use in graft for instantiation

    * ``_optional`` (set of str, or None): Optional field names for constructor

    * ``_read_only``: (set of str, or None): Field names that can't be given to constructor

    Examples
    --------
    >>> from descarteslabs.workflows import Struct, Str, Int
    >>> MyStructType = Struct[{'x': Str, 'y': Int}] # struct where type of 'x' is Str and type of 'y' is Int
    >>> instance = MyStructType(x="foo", y=1)
    >>> instance
    <descarteslabs.workflows.types.containers.struct.Struct[{'x': Str, 'y': Int}] object at 0x...>

    >>> from descarteslabs.workflows import Struct, Str, Int
    >>> class Foo(Struct[{'x': Int, 'y': Str}]): # doctest: +SKIP
    ...     _doc = {
    ...         'x': "the x field",
    ...         'y': "the y field, derived from x",
    ...     }
    ...     _constructor = "make_foo"
    ...     _optional = {'x'}
    ...     _read_only = {'y'}
    >>> my_struct = Foo(10) # doctest: +SKIP
    >>> my_struct.x.compute() # doctest: +SKIP
    10
    """

    # docstrings for struct fields; must be dict[str, str] or None
    _doc = None
    # function name to use in graft for instantiation; can be overridden in subclasses
    _constructor = "wf.struct"
    # optional arguments for __init__; must be a set or None
    _optional = None
    # read only attributes
    _read_only = None

    @staticmethod
    def __class_getitem_hook__(name, bases, dct, type_params):
        # Called when constructing a concrete subtype (`Struct[{'x': Int}]`).

        # We add `@property` getter functions for each field in the type params,
        # which makes the struct fields easier to document and introspect.
        dct.update(
            {
                field: _property_factory(field, field_cls)
                for field, field_cls in type_params[0].items()
            }
        )

        return name, bases, dct

    @classmethod
    def __init_subclass__(subcls, **kwargs):
        super().__init_subclass__(**kwargs)
        # If a subclass of a Struct sets the `_doc` class attribute,
        # we replace any field getters (created in `__class_getitem_hook__`)
        # with new field getters that include the given docstrings
        try:
            type_params = subcls._type_params[0]
        except IndexError:
            return

        docs = subcls._doc

        if docs is not None:
            for field, docstring in docs.items():
                if field not in type_params:
                    raise TypeError(
                        "Cannot document field {!r}: it does not exist in {}".format(
                            field, subcls
                        )
                    )

                field_cls = type_params[field]
                setattr(subcls, field, _property_factory(field, field_cls, docstring))

    def __new__(cls, *args, **kwargs):
        new = super(Struct, cls).__new__(cls)
        # Always create an `_items_cache` cache of looked-up attrs,
        # even when __init__ is bypassed by `from_graft`
        new._items_cache = {}
        return new

    # QUESTION(gabe): I'm not sure having __init__ take (and require) kwargs
    # for all the fields is the best move, since subclasses (which represent user-facing objects)
    # may want very different constructors, or to be cast-only and not even have a public constructor.
    # We'll see.
    def __init__(self, **kwargs):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic {}; the item types must be specified "
                "(like Struct[{}])".format(type(self).__name__, "{'a': Str, 'b': Int}")
            )
        promoted = self._promote_kwargs(
            kwargs, optional=self._optional, read_only=self._read_only
        )
        self.graft = client.apply_graft(self._constructor, **promoted)
        self.params = merge_params(self._constructor, *kwargs.values())
        # ^ NOTE: this would need to include the keys as well if proxytypes ever become hashable

        self._items_cache = promoted
        # ^ this _might_ be wrong, since the getattr graft won't include `getattr`
        # and just directly reference the promoted object, but that seems ok (while everything is immutable)

    @classmethod
    def _validate_params(cls, type_params):
        for type_param in type_params:
            assert isinstance(
                type_param, dict
            ), "Struct type parameters must be specified with a dictionary of field name to type"
            for key, param_cls in type_param.items():
                assert isinstance(
                    key, str
                ), "Field names must be strings, but '{}' is a {!r}".format(
                    key, type(key)
                )
                error_message = "Struct item type parameters must be Proxytypes but for field '{}', got {}".format(
                    key, param_cls
                )
                assert_is_proxytype(param_cls, error_message=error_message)

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Any):
            return obj.cast(cls)
        else:
            raise ProxyTypeError("Cannot promote {} to {}".format(obj, cls)) from None

    @classmethod
    def _promote_kwargs(cls, kwargs, optional=None, read_only=None):
        if optional is None:
            optional = set()
        if not isinstance(optional, set):
            raise TypeError(
                "Optional kwargs must be given as a set, not {}".format(optional)
            )
        if read_only is None:
            read_only = set()
        if not isinstance(read_only, set):
            raise TypeError(
                "Read only kwargs must be given as a set, not {}".format(read_only)
            )
        class_name = cls.__name__
        try:
            type_params = cls._type_params[0]
        except TypeError:
            raise TypeError(
                "Cannot instantiate a generic {}; the item types must be specified".format(
                    class_name
                )
            )

        missing_args = type_params.keys() - kwargs.keys() - optional - read_only

        if len(missing_args) > 0:
            raise ProxyTypeError(
                "Missing required keyword arguments to {}: {}".format(
                    class_name, ", ".join(map(repr, missing_args))
                )
            )

        provided_read_only_args = kwargs.keys() & read_only
        if len(provided_read_only_args) > 0:
            raise ProxyTypeError(
                "Read only keyword argument to {}: {}".format(
                    class_name, ", ".join(map(repr, provided_read_only_args))
                )
            )

        promoted_kwargs = {}
        for field_name, val in kwargs.items():
            try:
                field_cls = type_params[field_name]
            except KeyError:
                raise ProxyTypeError(
                    "{} has no field {!r}".format(class_name, field_name)
                )

            if val is None or isinstance(val, NoneType) and field_name in optional:
                continue
            try:
                promoted_val = field_cls._promote(val)
            except ProxyTypeError as e:
                raise ProxyTypeError(
                    "In field {!r} of {}, expected {}, but got {}: {}".format(
                        field_name, class_name, field_cls, type(val), val
                    ),
                    e,
                )
            promoted_kwargs[field_name] = promoted_val

        return promoted_kwargs

    def _attr(self, attr):
        try:
            item = self._items_cache[attr]
        except KeyError:
            try:
                value_type = self._type_params[0][attr]
            except KeyError:
                raise AttributeError(
                    "{} has no attribute {!r}".format(type(self).__name__, attr)
                )

            item = value_type._from_apply("wf.getattr", self, attr)
            self._items_cache[attr] = item
        return item
