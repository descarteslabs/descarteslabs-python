import re

from .attributes import AttributeValidationError, CatalogObjectReference, TypedAttribute
from .catalog_base import CatalogObject, _new_abstract_class
from .product import Product


class NamedIdAttribute(TypedAttribute):
    """str, immutable: An optional unique identifier for this object.

    The identifier for a named catalog object is the concatenation of the `product_id`
    and `name`, separated by a colon.  It will be generated from the `product_id` and
    the `name` if not provided.  Otherwise, the `name` and `product_id` are extracted
    from the `id`.  A :py:exc:`AttributeValidationError` will be raised if it conflicts
    with an existing `product_id` and/or `name`.
    """

    # Verifies that the id is a concatenation of product id and name
    # It will compare the `id` against the `name` and `product_id` attributes if
    # they are set.  It will set the `name` and `product_id` atttributes otherwise.

    def __init__(self):
        super(NamedIdAttribute, self).__init__(str, mutable=False, serializable=False)

    def __set__(self, obj, value, validate=True):
        last_colon = value.rfind(":")

        if last_colon == -1:
            raise AttributeValidationError(
                "The id must be a concatenation of a product id and a name, "
                "separated by a colon, not '{}'".format(value)
            )

        # Only update if it differs
        if value != obj.id:
            super(NamedIdAttribute, self).__set__(obj, value, validate=validate)

        # Some older images have colons in their names, so for existing data being
        # loaded from the service we can't make the assumption that we can recover
        # the name from the id.
        if not obj._saved:
            product_id = value[:last_colon]
            name = value[last_colon + 1 :]
            # Only update if it differs
            if product_id != obj.product_id:
                obj._get_attribute_type("product_id").__set__(
                    obj, product_id, validate=validate
                )
            if name != obj.name:
                obj._get_attribute_type("name").__set__(obj, name, validate=validate)


class NameAttribute(TypedAttribute):
    """str, immutable: The name of the catalog object.

    The name of a named catalog object is unique within a product and object type
    (images and bands).  The name can contain alphanumeric characters, ``-``, ``_``,
    and ``.`` up to 2000 characters.  If the `id` contains a name, it will be used
    instead.  Once set, it cannot be changed.

    *Sortable*.
    """

    # Sets the id if the `product_id` is already set."""

    def __init__(self):
        super(NameAttribute, self).__init__(str, mutable=False)

    def __set__(self, obj, value, validate=True):
        # Only update if it differs
        if value != obj.name:
            super(NameAttribute, self).__set__(obj, value, validate=validate)

        if value is not None and obj.id is None and obj.product_id:
            id_ = "{}:{}".format(obj.product_id, value)
            # Only update if it differs
            if id_ != obj.id:
                obj._get_attribute_type("id").__set__(obj, id_, validate=validate)


class ProductIdAttribute(TypedAttribute):
    """str, immutable: The id of the product this catalog object belongs to.

    If the `id` contains a product id, it will be used instead.  Once set, it cannot
    be changed.

    *Filterable, sortable*.
    """

    # Sets the id if the `name` is already set."""

    def __init__(self):
        super(ProductIdAttribute, self).__init__(str, mutable=False)

    def __set__(self, obj, value, validate=True):
        # Only update if it differs
        if value != obj.product_id:
            super(ProductIdAttribute, self).__set__(obj, value, validate=validate)

        if value is not None and obj.id is None and obj.name:
            id_ = "{}:{}".format(value, obj.name)
            # Only update if it differs
            if id_ != obj.id:
                obj._get_attribute_type("id").__set__(obj, id_, validate=validate)


class NamedCatalogObject(CatalogObject):
    """A catalog object with a required `name` and `product_id` instead of `id`.

    A catalog object without a required `id` but instead a required `name` and `product`
    or `product_id`.  The `id` is generated from the `product_id` and the `name`
    (`product_id`:`name`).  If the `id` is provided, it will be used to extract the
    `name` and `product_id`, if available.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.

    Example
    -------
    Any combination that will yield the image name and the product id will work, but
    the preferred way is using the `name` and `product`:

    >>> from descarteslabs.catalog import Product, Image
    >>> product_id = "some_org:some_product_name"
    >>> product = Product.get(product_id) # doctest: +SKIP
    >>> image_name = "some_image_name"
    >>> # Preferred
    >>> product = Product(id=product_id)
    >>> image = Image(name=image_name, product=product) # doctest: +SKIP
    >>> # Also possible...
    >>> image_id = "{}:{}".format(product.id, image_name)
    >>> image = Image(id=image_id)
    """

    _invalid_sequence_pattern_for_name = re.compile(r"[^a-zA-Z0-9_.-]+")

    id = NamedIdAttribute()
    name = NameAttribute()
    product_id = ProductIdAttribute()
    product = CatalogObjectReference(
        Product,
        mutable=False,
        sticky=True,
        doc="""
        Product, immutable: The product instance this catalog object belongs to.

        If given, it is used to retrieve the `product_id`.

        *Filterable*.
        """,
    )

    def __new__(cls, *args, **kwargs):
        return _new_abstract_class(cls, NamedCatalogObject)

    def __init__(self, **kwargs):
        product_id = kwargs.get("product_id")
        if product_id is None:
            product = kwargs.get("product")
            if product is not None:
                kwargs["product_id"] = product.id

        super(NamedCatalogObject, self).__init__(**kwargs)

    @classmethod
    def make_valid_name(cls, name):
        """Replace invalid characters in the given name and return a valid name.

        Replace any sequence of invalid characters in a string with a single `_`
        character to create a valid `~Image.name` for `Band` or `Image`.  Since the
        Band and Image names have a limited character set, this method will replace
        any sequence of characters outside that character set with a single ``_``
        character.  The returned string is a safe name to use for a `Band` or `Image`.
        The given string is unchanged.

        Note that it is possible that two unique invalid names may turn into duplicate
        valid names if the uniqueness is located in the same sequence of invalid
        characters.

        Parameters
        ----------
        name : str
            A `~Image.name` for a `Band` or `Image` that may contain invalid characters.

        Returns
        -------
        str
            A `~Image.name` for a `Band` or `Image` that does not contain any invalid
            characters.

        Example
        -------
        >>> from descarteslabs.catalog import SpectralBand, Band
        >>> name = "This is ań @#$^*% ïñvalid name!!!!"
        >>> band = SpectralBand()
        >>> band.name = Band.make_valid_name(name)
        >>> band.name
        'This_is_a_valid_name_'
        """
        return cls._invalid_sequence_pattern_for_name.sub("_", name)
