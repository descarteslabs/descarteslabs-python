from .attributes import Attribute, AttributeValidationError, CatalogObjectReference
from .catalog_base import CatalogObject
from .product import Product

from .attributes import DocumentState  # noqa : F104 -- This is for the documentation


class NamedIdAttribute(Attribute):
    """Verifies that the id is a concatenation of product id and name

    It will compare the `id` against the `name` and `product_id` attributes if
    they are set.  It will set the `name` and `product_id` atttributes otherwise.
    """

    def __init__(self):
        super(NamedIdAttribute, self).__init__(_mutable=False, _serializable=False)

    def __set__(self, obj, value, validate=True):
        last_colon = value.rfind(":")

        if last_colon == -1:
            raise AttributeValidationError(
                "The id must be a concatenation of a product id and a name, "
                "separated by a colon, not '{}'".format(value)
            )

        super(NamedIdAttribute, self).__set__(obj, value, validate=validate)

        # Some older images have colons in their names, so for existing data being
        # loaded from the service we can't make the assumption that we can recover
        # the name from the id.
        if not obj._saved:
            product_id = value[:last_colon]
            name_part = value[last_colon + 1 :]
            obj._get_attribute_type("product_id").__set__(
                obj, product_id, validate=validate
            )
            obj._get_attribute_type("name").__set__(obj, name_part, validate=validate)


class NameAttribute(Attribute):
    """Sets the id if the `product_id` is already set."""

    def __init__(self):
        super(NameAttribute, self).__init__(_mutable=False)

    def __set__(self, obj, value, validate=True):
        super(NameAttribute, self).__set__(obj, value, validate=validate)
        if value is not None and obj.id is None and obj.product_id:
            obj._get_attribute_type("id").__set__(
                obj, "{}:{}".format(obj.product_id, value), validate=validate
            )


class ProductIdAttribute(Attribute):
    """Sets the id if the `name` is already set."""

    def __init__(self):
        super(ProductIdAttribute, self).__init__(_mutable=False)

    def __set__(self, obj, value, validate=True):
        super(ProductIdAttribute, self).__set__(obj, value, validate=validate)
        if value is not None and obj.id is None and obj.name:
            obj._get_attribute_type("id").__set__(
                obj, "{}:{}".format(value, obj.name), validate=validate
            )


class NamedCatalogObject(CatalogObject):
    """A catalog object with a required `name` and `product_id` instead of `id`.

    A catalog object without a required `id` but instead a required `name` and `product`
    or `product_id`.  The `id` is generated from the `product_id` and the `name`
    (`product_id`:`name`).  If the `id` is provided, it will be used to extract the
    `name` and `product_id`, if available.

    Parameters
    ----------
    kwargs : dict
        With the exception of readonly attributes
        (:py:attr:`~descarteslabs.catalog.CatalogObject.created`,
        :py:attr:`~descarteslabs.catalog.CatalogObject.modified`), any
        (inherited) attribute listed below can also be used as a keyword argument.

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base class:

    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    **The attributes documented below are shared by all named catalog objects,
    namely Band and Image.**

    Attributes
    ----------
    id : str
        Immutable: An optional unique identifier for this object, a concatenation of
        the `product_id` and `name`, separated by a colon.  It will be generated from
        the `product_id` and the `name` if not provided.  Otherwise, the `name` and
        `product_id` are extracted from the `id`.  A :py:exc:`AttributeValidationError`
        will be raised if it conflicts with an existing `product_id` and/or `name`.
    name : str
        Required, immutable: The name of the catalog object, unique within a product.
        The name can contain alphanumeric characters, ``-``, ``_``, and ``.``.  If the
        `id` contains a name, it will be used instead.  Once set, it cannot be changed.
        *Sortable*.
    product_id : str
        Required, immutable: The id of the product this catalog object belongs to.  If
        the `id` contains a product id, it will be used instead.  Once set, it cannot
        be changed.
        *Filterable, sortable*.
    product : Product
        The representation of the product this catalog object belongs to.
        If given, it is used to retrieve the `product_id`.
        *Filterable*.

    Example
    -------
    Any combination that will yield the image name and the product id will work, but
    the preferred way is using the `name` and `product`:

    >>> product_id = "some_org:some_product_name"
    >>> product = Product.get(product_id)
    >>> image_name = "some_image_name"
    >>> # Preferred
    >>> image = Image(name=image_name, product=product)
    >>> # Also possible...
    >>> image_id = "{}:{}".format(product.id, image_name)
    >>> image = Image(id=image_id)

    """

    id = NamedIdAttribute()
    name = NameAttribute()
    product_id = ProductIdAttribute()
    product = CatalogObjectReference(Product, _mutable=False, _sticky=True)

    def __init__(self, **kwargs):
        product_id = kwargs.get("product_id")
        if product_id is None:
            product = kwargs.get("product")
            if product is not None:
                kwargs["product_id"] = product.id

        super(NamedCatalogObject, self).__init__(**kwargs)
