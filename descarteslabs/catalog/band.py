from enum import Enum
from collections.abc import Iterable, Mapping, MutableMapping

from descarteslabs.common.property_filtering import GenericProperties
from .catalog_base import CatalogObject, _new_abstract_class
from .named_catalog_base import NamedCatalogObject
from .attributes import (
    Attribute,
    EnumAttribute,
    Resolution,
    BooleanAttribute,
    ListAttribute,
    TupleAttribute,
    TypedAttribute,
    ModelAttribute,
    MappingAttribute,
    AttributeValidationError,
)


properties = GenericProperties()


class DataType(str, Enum):
    """Valid data types for bands.

    Attributes
    ----------
    BYTE : enum
        An 8 bit unsigned integer value.
    UINT16 : enum
        A 16 bit unsigned integer value.
    INT16 : enum
        A 16 bit signed integer value.
    UINT32 : enum
        A 32 bit unsigned integer value.
    INT32 : enum
        A 32 bit signed integer value.
    FLOAT32 : enum
        A 32 bit single-precision floating-point format value.
    FLOAT64 : enum
        A 64 bit double-precision floating-point format value.
    """

    BYTE = "Byte"
    UINT16 = "UInt16"
    INT16 = "Int16"
    UINT32 = "UInt32"
    INT32 = "Int32"
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"


class BandType(str, Enum):
    """Types of bands with different data interpretation.

    The type of band is represented in the specific Band class being used
    and is only for informative purposes.

    Attributes
    ----------
    CLASS : enum
        A band that maps a finite set of values that may not be continuous.
    SPECTRAL : enum
        A band that lies somewhere on the visible/NIR/SWIR electro-optical wavelength
        spectrum.
    MASK : enum
        A binary band where by convention a 0 means masked and 1 means non-masked.
    MICROWAVE : enum
        A band that lies in the microwave spectrum, often from SAR or passive radar
        sensors.
    GENERIC : enum
        An unspecified kind of band not fitting any other type.
    """

    CLASS = "class"
    SPECTRAL = "spectral"
    MASK = "mask"
    MICROWAVE = "microwave"
    GENERIC = "generic"


class Colormap(str, Enum):
    """Predefined colormaps available to assign to bands.

    Most of these colormaps correspond directly to the built-in colormaps of the
    same name in matplotlib. See
    https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html for an
    overview and visual examples.

    Attributes
    ----------
    MAGMA : enum
        A perceptually uniform sequential colormap, equivalent to matplotlib's
        built-in "magma"
    INFERNO : enum
        A perceptually uniform sequential colormap, equivalent to matplotlib's
        built-in "inferno"
    PLASMA : enum
        A perceptually uniform sequential colormap, equivalent to matplotlib's
        built-in "plasma"
    VIRIDIS : enum
        A perceptually uniform sequential colormap, equivalent to matplotlib's
        built-in "viridis"
    COOL : enum
        A sequential colormap, equivalent to matplotlib's built-in "cool"
    HOT : enum
        A sequential colormap, equivalent to matplotlib's built-in "hot"
    COOLWARM : enum
        A diverging colormap, equivalent to matplotlib's built-in "coolwarm"
    BWR : enum
        A diverging colormap (blue-white-red), equivalent to matplotlib's
        built-in "bwr"
    GIST_EARTH : enum
        A colormap designed to represent topography and water depths together,
        equivalent to matplotlib's built-in "gist_earth"
    TERRAIN : enum
        A colormap designed to represent topography and water depths together,
        equivalent to matplotlib's built-in "terrain"
    CDL : enum
        A standard colormap used in Cropland Data Layer (CDL) products, with a
        distinct color for each class in such products
    """

    MAGMA = "magma"
    INFERNO = "inferno"
    PLASMA = "plasma"
    VIRIDIS = "viridis"
    COOL = "cool"
    HOT = "hot"
    COOLWARM = "coolwarm"
    BWR = "bwr"
    GIST_EARTH = "gist_earth"
    TERRAIN = "terrain"
    CDL = "cdl"


class ProcessingStepAttribute(MappingAttribute):
    """Processing Levels Step.

    Attributes
    ----------
    function : str
        Name of the processing function to apply. Required.
    parameter : str
        Name of the parameter in the image metadata containing
        the coefficients for the processing function. Required.
    index : int
        Optional index into the named parameter (an array) for the band.
    """

    function = TypedAttribute(str)
    parameter = TypedAttribute(str)
    index = TypedAttribute(int)


class ProcessingLevelsAttribute(ModelAttribute, MutableMapping):
    """An attribute that contains properties (key/value pairs).

    Can be set using a dictionary of items or any `Mapping`, or an instance of this
    attribute.  All keys must be string and values can be string or an iterable
    of `ProcessingStepAttribute` items (or compatible mapping).
    `ProcessingLevelsAttribute` behaves similar to dictionaries.
    """

    # this value is ONLY used for for instances of the attribute that
    # are attached to class definitions. It's confusing to put this
    # instantiation into __init__, because the value is only ever set
    # from AttributeMeta.__new__, after it's already been instantiated
    _attribute_name = None

    def __init__(self, value=None, validate=True, **kwargs):
        self._items = {}

        super(ProcessingLevelsAttribute, self).__init__(**kwargs)

        if value is not None:
            # we always validate, to correctly coerce the values
            value = {
                key: self.validate_key_and_value(key, val) for key, val in value.items()
            }

            self._items.update(value)

    def __repr__(self):
        return "{}{}{}".format(
            "{",
            ", ".join(
                [
                    "{}: {}".format(repr(key), repr(value))
                    for key, value in self._items.items()
                ]
            ),
            "}",
        )

    def validate_key_and_value(self, key, value):
        """Validate the key and value.

        The key must be a string, and the value either a string or an iterable of
        `ProcessingStepAttribute` items or a compatible mapping.

        Returns a fully formed value (a string or a ListAttribute of
        `ProcessingStepAttribute` items)
        """
        if not isinstance(key, str):
            raise AttributeValidationError(
                "Keys for property {} must be strings: {}".format(
                    self._attribute_name, key
                )
            )
        if isinstance(value, str):
            return value
        elif isinstance(value, ListAttribute) and all(
            map(lambda x: isinstance(x, ProcessingStepAttribute), value)
        ):
            value._add_model_object(self)
            return value
        elif isinstance(value, Iterable):
            items = []
            for v in value:
                if isinstance(v, ProcessingStepAttribute):
                    items.append(v)
                elif isinstance(v, Mapping):
                    try:
                        items.append(ProcessingStepAttribute(**v))
                    except AttributeError as ex:
                        raise AttributeValidationError(
                            "The value for property {} with key {} must"
                            " conform to a valid ProcessingStepAttribute: {}: {}".format(
                                self._attribute_name, key, v, ex
                            )
                        )
                else:
                    break
            else:
                value = ListAttribute(ProcessingStepAttribute, items=items)
                value._add_model_object(self)
                return value
        raise AttributeValidationError(
            "The value for property {} with key {} must be a string"
            " or an iterable of ProcessingStepAttribute: {}".format(
                self._attribute_name, key, value
            )
        )

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        if value is None:
            return None

        # Shallow copy for strings, deserialize ListAttributes
        return {
            k: v
            if isinstance(v, str)
            else v.serialize(v, jsonapi_format=jsonapi_format)
            for k, v in value.items()
        }

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Parameters
        ----------
        value : dict or `ProcessingLevelsAttribute`
            A set of values to use to initialize a new `ProcessingLevelsAttribute`
            instance.  All keys must be strings, and values can be strings or numbers.

        Returns
        -------
        ProcessingLevelsAttribute
            A `ProcessingLevelsAttribute` with the given items.

        Raises
        ------
        AttributeValidationError
            If the value is not a mapping or any of the keys are not strings, or any
            of the values are not strings or numbers.
        """
        if value is None:
            return None

        if isinstance(value, ProcessingLevelsAttribute):
            return value

        if validate:
            if not isinstance(value, Mapping):
                raise AttributeValidationError(
                    "A ProcessingLevelsAttribute expects a mapping: {}".format(
                        self._attribute_name
                    )
                )

            value = {
                key: self.validate_key_and_value(key, val) for key, val in value.items()
            }

        return ProcessingLevelsAttribute(
            value, validate=validate, **self._get_attr_params()
        )

    # Mapping methods

    def __getitem__(self, key):
        return self._items[key]

    def __setitem__(self, key, value):
        self._raise_if_immutable_or_readonly("set")
        value = self.validate_key_and_value(key, value)

        old_value = self._items.get(key, None)
        changed = key not in self._items or old_value != value
        self._set_modified(changed=changed)
        self._items[key] = value
        if isinstance(old_value, ModelAttribute):
            old_value._remove_model_object(self)

    def __delitem__(self, key):
        self._raise_if_immutable_or_readonly("delete")
        if key in self._items:
            self._set_modified(changed=True)
        old_value = self._items.pop(key)
        if isinstance(old_value, ModelAttribute):
            old_value._remove_model_object(self)

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


class DerivedParamsAttribute(MappingAttribute):
    """Derived Band Parameters Attribute.

    Attributes
    ----------
    function : str
        Name of the function to apply. Required.
    bands : list(str)
        Names of the bands used as input to the function. Required.
    source_type : int
        Optional index into the named parameter (an array) for the band.
    """

    function = TypedAttribute(str)
    bands = ListAttribute(TypedAttribute(str), validate=True)
    source_type = EnumAttribute(
        DataType,
        doc="str or DataType: The datatype for extracting pixels from the source bands.",
    )

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Parameters
        ----------
        values : dict or MappingAttribute
            The values to use to initialize a new MappingAttribute.

        Returns
        -------
        DerivedParamsAttribute
            A `DerivedParamsAttribute` instance with the given values.

        Raises
        ------
        AttributeValidationError
            If the value is not a `DerivedParamsAttribute` or a mapping with
            a `function`, `bands`, and (optionally) `source_type key.
        """
        result = super(DerivedParamsAttribute, self).deserialize(value, validate)

        if result:
            if not result.function:
                raise AttributeValidationError("'function' field required.")
            if not result.bands:
                raise AttributeValidationError("'bands' field required.")

        return result


class Band(NamedCatalogObject):
    """A data band in images of a specific product.

    This is an abstract class that cannot be instantiated, but can be used for searching
    across all types of bands.  The concrete bands are represented by the derived
    classes.

    Common attributes:
    :attr:`~descarteslabs.catalog.GenericBand.id`,
    :attr:`~descarteslabs.catalog.GenericBand.name`,
    :attr:`~descarteslabs.catalog.GenericBand.product_id`,
    :attr:`~descarteslabs.catalog.GenericBand.description`,
    :attr:`~descarteslabs.catalog.GenericBand.type`,
    :attr:`~descarteslabs.catalog.GenericBand.sort_order`,
    :attr:`~descarteslabs.catalog.GenericBand.vendor_order`,
    :attr:`~descarteslabs.catalog.GenericBand.data_type`,
    :attr:`~descarteslabs.catalog.GenericBand.nodata`,
    :attr:`~descarteslabs.catalog.GenericBand.data_range`,
    :attr:`~descarteslabs.catalog.GenericBand.display_range`,
    :attr:`~descarteslabs.catalog.GenericBand.resolution`,
    :attr:`~descarteslabs.catalog.GenericBand.band_index`,
    :attr:`~descarteslabs.catalog.GenericBand.file_index`,
    :attr:`~descarteslabs.catalog.GenericBand.jpx_layer_index`.
    :attr:`~descarteslabs.catalog.GenericBand.vendor_band_name`.

    To create a new band instantiate one of those specialized classes:

    * `SpectralBand`: A band that lies somewhere on the visible/NIR/SWIR electro-optical
      wavelength spectrum. Specific attributes:
      :attr:`~SpectralBand.physical_range`,
      :attr:`~SpectralBand.physical_range_unit`,
      :attr:`~SpectralBand.wavelength_nm_center`,
      :attr:`~SpectralBand.wavelength_nm_min`,
      :attr:`~SpectralBand.wavelength_nm_max`,
      :attr:`~SpectralBand.wavelength_nm_fwhm`,
      :attr:`~descarteslabs.catalog.GenericBand.processing_levels`,
      :attr:`~descarteslabs.catalog.GenericBand.derived_params`.
    * `MicrowaveBand`: A band that lies in the microwave spectrum, often from SAR or
      passive radar sensors. Specific attributes:
      :attr:`~MicrowaveBand.frequency`,
      :attr:`~MicrowaveBand.bandwidth`,
      :attr:`~MicrowaveBand.physical_range`,
      :attr:`~MicrowaveBand.physical_range_unit`.
    * `MaskBand`: A binary band where by convention a 0 means masked and 1 means
      non-masked. The :attr:`~Band.data_range` and :attr:`~Band.display_range` for
      masks is implicitly ``[0, 1]``. Specific attributes:
      :attr:`~MaskBand.is_alpha`.
    * `ClassBand`: A band that maps a finite set of values that may not be continuous to
      classification categories (e.g. a land use classification). A visualization with
      straight pixel values is typically not useful, so commonly a
      :attr:`~ClassBand.colormap` is used. Specific attributes:
      :attr:`~ClassBand.colormap`, :attr:`~ClassBand.colormap_name`,
      :attr:`~ClassBand.class_labels`.
    * `GenericBand`: A generic type for bands that are not represented by the other band
      types, e.g., mapping physical values like temperature or angles. Specific
      attributes:
      :attr:`~GenericBand.colormap`,
      :attr:`~GenericBand.colormap_name`,
      :attr:`~GenericBand.physical_range`,
      :attr:`~GenericBand.physical_range_unit`,
      :attr:`~descarteslabs.catalog.GenericBand.processing_levels`,
      :attr:`~descarteslabs.catalog.GenericBand.derived_params`.
    """

    _DOC_DESCRIPTION = """A description with further details on the band.

        The description can be up to 80,000 characters and is used by
        :py:meth:`Search.find_text`.

        *Searchable*
        """
    _DOC_DATATYPE = "The data type for pixel values in this band."
    _DOC_DATARANGE = """The range of pixel values stored in the band.

        The two floats are the minimum and maximum pixel values stored in this band.
        """
    _DOC_COLORMAPNAME = """str or Colormap, optional: Name of a predefined colormap for display purposes.

        The colormap is applied when this band is rastered by itself in PNG or TIFF
        format, including in UIs where imagery is visualized.
        """
    _DOC_COLORMAP = """list(tuple), optional: A custom colormap for this band.

        A list of tuples, where each nested tuple is a 4-tuple of RGBA values to map
        pixels whose value is the index of the list.  E.g.  the colormap ``[(100, 20,
        200, 255)]`` would map pixels whose value is 0 in the original band to the
        RGBA color defined by ``(100, 20, 200, 255)``.  The number of 4-tuples provided
        can be up to the maximum of this band's data range.  Omitted values will map
        to black by default.
        """
    _DOC_PHYSICALRANGE = (
        "tuple(float, float), optional: A physical range that pixel values map to."
    )

    _doc_type = "band"
    _url = "/bands"
    _derived_type_switch = "type"
    _default_includes = ["product"]

    description = Attribute(doc="str, optional: " + _DOC_DESCRIPTION)
    type = EnumAttribute(
        BandType,
        doc="""str or BandType: The type of this band, directly corresponding to a `Band` derived class.

        The derived classes are `SpectralBand`, `MicrowaveBand`, `MaskBand`,
        `ClassBand`, and `GenericBand`.  The type never needs to be set explicitly,
        this attribute is implied by the derived class used.  The type of a band does
        not necessarily affect how it is rastered, it mainly conveys useful information
        about the data it contains.

        *Filterable*.
        """,
    )
    sort_order = TypedAttribute(
        int,
        doc="""int, optional: A number defining the default sort order for bands within a product.

        If not set for newly created bands, this will default to the current maximum
        sort order + 1 in the product.

        *Sortable*.
        """,
    )
    vendor_order = TypedAttribute(
        int,
        doc="""int, optional: A number defining the ordering of bands within a product
        as defined by the data vendor. 1-based. Used for indexing ``c6s_dlsr``.
        Generally only used internally by certain core products.

        *Sortable*.
        """,
    )
    data_type = EnumAttribute(DataType, doc="str or DataType: " + _DOC_DATATYPE)
    nodata = Attribute(
        doc="""float, optional: A value representing missing data in a pixel in this band."""
    )
    data_range = TupleAttribute(
        min_length=2,
        max_length=2,
        coerce=True,
        attribute_type=float,
        doc="tuple(float, float): " + _DOC_DATARANGE,
    )
    display_range = TupleAttribute(
        min_length=2,
        max_length=2,
        coerce=True,
        attribute_type=float,
        doc="""tuple(float, float): The range of pixel values for display purposes.

        The two floats are the minimum and maximum values indicating a default reasonable
        range of pixel values usd when rastering this band for display purposes.
        """,
    )
    resolution = Resolution(
        doc="""Resolution, optional: The spatial resolution of this band.

        *Filterable, sortable*.
        """
    )
    band_index = TypedAttribute(
        int, doc="int: The 0-based index into the source data to access this band."
    )
    file_index = Attribute(
        doc="""int, optional: The 0-based index into the list of source files.

        If there are multiple files, it maps the band index to the file index.  It defaults
        to 0 (first file).
        """
    )
    jpx_layer_index = TypedAttribute(
        int,
        doc="""int, optional: The 0-based layer index if the source data is JPEG2000 with layers.

        Defaults to 0.
        """,
    )
    vendor_band_name = TypedAttribute(
        str,
        doc="""str, optional: The name of the band in the source file.

        Some source file types require that the band be indexed by name rather than by the ``band_index``.
        """,
    )
    processing_levels = ProcessingLevelsAttribute()
    derived_params = DerivedParamsAttribute()

    def __new__(cls, *args, **kwargs):
        return _new_abstract_class(cls, Band)

    def __init__(self, **kwargs):
        if self._derived_type_switch not in kwargs:
            kwargs[self._derived_type_switch] = self._derived_type

        super(Band, self).__init__(**kwargs)

    @classmethod
    def search(cls, client=None):
        """A search query for all bands.

        Returns an instance of the
        :py:class:`~descarteslabs.catalog.Search` class configured for
        searching bands.  Call this on the :py:class:`Band` base class to search all
        types of bands or classes :py:class:`SpectralBand`, :py:class:`MicrowaveBand`,
        :py:class:`MaskBand`, :py:class:`ClassBand` and :py:class:`GenericBand` to search
        only a specific type of band.


        Parameters
        ----------
        client : :py:class:`CatalogClient`
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.Search`
            An instance of the :py:class:`~descarteslabs.catalog.Search` class
        """
        search = super(Band, cls).search(client)
        if cls._derived_type:
            search = search.filter(properties.type == cls._derived_type)
        return search


class SpectralBand(Band):
    """A band that lies somewhere on the visible/NIR/SWIR electro-optical wavelength spectrum.

    Instantiating a spectral band indicates that you want to create a *new* Descartes
    Labs catalog spectral band.  If you instead want to retrieve an existing catalog
    spectral band use `Band.get() <descarteslabs.catalog.Band.get>`, or if
    you're not sure use `SpectralBand.get_or_create()
    <descarteslabs.catalog.SpectralBand.get_or_create>`.  You can also use
    `Band.search() <descarteslabs.catalog.Band.search>`.  Also see the example
    for :py:meth:`~descarteslabs.catalog.Band.save`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.  Also see
        `~SpectralBand.ATTRIBUTES`.
    """

    _derived_type = BandType.SPECTRAL.value

    physical_range = Attribute(doc=Band._DOC_PHYSICALRANGE)
    physical_range_unit = Attribute(doc="str, optional: Unit of the physical range.")
    wavelength_nm_center = Attribute(
        doc="""float, optional: Weighted center of min/max responsiveness of the band, in nm.

        *Filterable, sortable*.
        """
    )
    wavelength_nm_min = Attribute(
        doc="""float, optional: Minimum wavelength this band is sensitive to, in nm.

        *Filterable, sortable*.
        """
    )
    wavelength_nm_max = Attribute(
        doc="""float, optional: Maximum wavelength this band is sensitive to, in nm.

            *Filterable, sortable*.
        """
    )
    wavelength_nm_fwhm = Attribute(
        doc="""float, optional: Full width at half maximum value of the wavelength spread, in nm.

        *Filterable, sortable*.
        """
    )


class MicrowaveBand(Band):
    """A band that lies in the microwave spectrum, often from SAR or passive radar sensors.

    Instantiating a microwave band indicates that you want to create a *new* Descartes
    Labs catalog microwave band.  If you instead want to retrieve an existing catalog
    microwave band use `Band.get() <descarteslabs.catalog.Band.get>`, or if
    you're not sure use `MicrowaveBand.get_or_create()
    <descarteslabs.catalog.MicrowaveBand.get_or_create>`.  You can also use
    `Band.search() <descarteslabs.catalog.Band.search>`.  Also see the example
    for :py:meth:`~descarteslabs.catalog.Band.save`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.  Also see
        `~MicrowaveBand.ATTRIBUTES`.
    """

    _derived_type = BandType.MICROWAVE.value

    frequency = Attribute(
        doc="""float, optional: Center frequency of the observed microwave in GHz.

        *Filterable, sortable*.
        """
    )
    bandwidth = Attribute(
        doc="""float, optional: Chirp bandwidth of the sensor in MHz.

        *Filterable, sortable*.
        """
    )
    physical_range = Attribute(doc=Band._DOC_PHYSICALRANGE)
    physical_range_unit = Attribute(doc="str, optional: Unit of the physical range.")


class MaskBand(Band):
    """A binary band where by convention a 0 means masked and 1 means non-masked.

    The `data_range` and `display_range` for masks is implicitly ``(0, 1)``.

    Instantiating a mask band indicates that you want to create a *new* Descartes
    Labs catalog mask band.  If you instead want to retrieve an existing catalog
    mask band use `Band.get() <descarteslabs.catalog.Band.get>`, or if
    you're not sure use `MaskBand.get_or_create()
    <descarteslabs.catalog.MaskBand.get_or_create>`.  You can also use
    `Band.search() <descarteslabs.catalog.Band.search>`.  Also see the example
    for :py:meth:`~descarteslabs.catalog.Band.save`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.  Also see
        `~MaskBand.ATTRIBUTES`.
    """

    _derived_type = BandType.MASK.value

    is_alpha = BooleanAttribute(
        doc="""bool, optional: Whether this band should be useable as an alpha band during rastering.

        This enables special behavior for this band during rastering.  If this is
        ``True`` and the band appears as the last band in a raster operation (such as
        :meth:`descarteslabs.scenes.scenecollection.SceneCollection.mosaic` or
        :meth:`descarteslabs.scenes.scenecollection.SceneCollection.stack`) pixels
        with a value of 0 in this band will be treated as transparent.
        """
    )
    data_range = Attribute(mutable=False, doc="tuple(float, float), readonly: [0, 1].")
    display_range = Attribute(
        mutable=False, doc="tuple(float, float), readonly: [0, 1]."
    )


class ClassBand(Band):
    """A band that maps a finite set of values that may not be continuous.

    For example land use classification.  A visualization with straight pixel values
    is typically not useful, so commonly a colormap is used.

    Instantiating a class band indicates that you want to create a *new* Descartes
    Labs catalog class band.  If you instead want to retrieve an existing catalog
    class band use `Band.get() <descarteslabs.catalog.Band.get>`, or if
    you're not sure use `ClassBand.get_or_create()
    <descarteslabs.catalog.ClassBand.get_or_create>`.  You can also use
    `Band.search() <descarteslabs.catalog.Band.search>`.  Also see the example
    for :py:meth:`~descarteslabs.catalog.Band.save`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.  Also see
        `~ClassBand.ATTRIBUTES`.
    """

    _derived_type = BandType.CLASS.value

    colormap_name = EnumAttribute(Colormap, doc=Band._DOC_COLORMAPNAME)
    colormap = Attribute(doc=Band._DOC_COLORMAP)
    class_labels = ListAttribute(
        TypedAttribute(str),
        doc="""list(str or None), optional: A list of labels.

        A list of labels where each element is a name for the class with the value at
        that index.  Elements can be null if there is no label at that value.
        """,
    )


class GenericBand(Band):
    """A generic kind of band not fitting any other type.

    For example mapping physical values like temperature or angles.

    Instantiating a generic band indicates that you want to create a *new* Descartes
    Labs catalog generic band.  If you instead want to retrieve an existing catalog
    generic band use `Band.get() <descarteslabs.catalog.Band.get>`, or if
    you're not sure use `GenericBand.get_or_create()
    <descarteslabs.catalog.GenericBand.get_or_create>`.  You can also use
    `Band.search() <descarteslabs.catalog.Band.search>`.  Also see the example
    for :py:meth:`~descarteslabs.catalog.Band.save`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.  Also see
        `~GenericBand.ATTRIBUTES`.
    """

    _derived_type = BandType.GENERIC.value

    physical_range = Attribute(doc=Band._DOC_PHYSICALRANGE)
    physical_range_unit = Attribute(doc="str, optional: Unit of the physical range.")
    colormap_name = EnumAttribute(Colormap, doc=Band._DOC_COLORMAPNAME)
    colormap = Attribute(doc=Band._DOC_COLORMAP)


class DerivedBand(CatalogObject):
    """A band with pixel values computed from the data in other bands.

    A type of band that is the result of a pixel function applied to one or more
    existing bands. This object type only supports read operations;
    they cannot be created, updated, or deleted using this client.

    Instantiating a derived band can only be done through `Band.get()
    <descarteslabs.catalog.Band.get>`, or `Band.search()
    <descarteslabs.catalog.Band.search>`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict, optional
        You cannot set any additional keyword arguments as a derived band is readonly.
    """

    _doc_type = "derived_band"
    _url = "/derived_bands"

    name = Attribute(
        readonly=True,
        doc="""str, readonly: The name of the derived band, globally unique.

        *Filterable, sortable*.
        """,
    )
    description = Attribute(
        readonly=True, doc="str, readonly: " + Band._DOC_DESCRIPTION
    )
    data_type = EnumAttribute(
        DataType, readonly=True, doc="str or DataType, readonly: " + Band._DOC_DATATYPE
    )
    data_range = Attribute(
        readonly=True, doc="tuple(float, float), readonly: " + Band._DOC_DATARANGE
    )
    physical_range = Attribute(readonly=True, doc=Band._DOC_PHYSICALRANGE)
    bands = Attribute(
        readonly=True,
        doc="""list(str), readonly: List of bands used in the derived band pixel function.

        *Filterable*
        """,
    )
    function_name = Attribute(
        readonly=True,
        doc="str, readonly: Name of the function applied to create this derived band.",
    )

    def update(self, **kwargs):
        """You cannot update a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.
        """
        raise NotImplementedError("Updating DerivedBands is not permitted")

    def save(self):
        """You cannot save a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.
        """
        raise NotImplementedError("Saving DerivedBands is not permitted")

    @classmethod
    def delete(cls, id, client=None):
        """You cannot delete a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.
        """
        raise NotImplementedError("Deleting DerivedBands is not permitted")

    def _instance_delete(self):
        """You cannot delete a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.
        """
        raise NotImplementedError("Deleting DerivedBands is not permitted")
