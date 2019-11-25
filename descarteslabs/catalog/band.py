from enum import Enum

from descarteslabs.common.property_filtering import GenericProperties
from .catalog_base import CatalogObject
from .named_catalog_base import NamedCatalogObject
from .attributes import Attribute, EnumAttribute, Resolution


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


class Band(NamedCatalogObject):
    """A data band in images of a specific product.

    This is an abstract class that cannot be instantiated, but can be used for searching
    across all types of bands.  The concrete bands are represented by the derived
    classes.  To create a new band instantiate one of those specialized classes:

    * `SpectralBand`: A band that lies somewhere on the visible/NIR/SWIR electro-optical
      wavelength spectrum. Specific attributes:
      :attr:`~SpectralBand.wavelength_nm_center`,
      :attr:`~SpectralBand.wavelength_nm_min`,
      :attr:`~SpectralBand.wavelength_nm_max`,
      :attr:`~SpectralBand.wavelength_nm_fwhm`
    * `MicrowaveBand`: A band that lies in the microwave spectrum, often from SAR or
      passive radar sensors. Specific attributes: :attr:`~MicrowaveBand.frequency`,
      :attr:`~MicrowaveBand.bandwidth`
    * `MaskBand`: A binary band where by convention a 0 means masked and 1 means
      non-masked. The :attr:`~Band.data_range` and :attr:`~Band.display_range` for
      masks is implicitly ``[0, 1]``. Specific attributes::attr:`~MaskBand.is_alpha`
    * `ClassBand`: A band that maps a finite set of values that may not be continuous to
      classification categories (e.g. a land use classification). A visualization with
      straight pixel values is typically not useful, so commonly a
      :attr:`~ClassBand.colormap` is used. Specific attributes:
      :attr:`~ClassBand.colormap`, :attr:`~ClassBand.colormap_name`,
      :attr:`~ClassBand.class_labels`
    * `GenericBand`: A generic type for bands that are not represented by the other band
      types, e.g., mapping physical values like temperature or angles. Specific
      attributes: :attr:`~GenericBand.colormap`, :attr:`~GenericBand.colormap_name`,
      :attr:`~GenericBand.physical_range`, :attr:`~GenericBand.physical_range_unit`

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
    base classes:

    * :py:class:`descarteslabs.catalog.NamedCatalogObject`
    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    **The attributes documented below are shared by all band objects,
    namely SpectralBand, MicrowaveBand, MaskBand, ClassBand, and GenericBand.**

    Attributes
    ----------
    description : str
        A description with further details on the band
    type : str, BandType
        The type of this band, directly corresponding to a `Band` subclass
        (:py:class:`SpectralBand`, :py:class:`MicrowaveBand`, :py:class:`MaskBand`,
        :py:class:`ClassBand`, :py:class:`GenericBand`). Never needs to be set
        explicitly, this attribute is implied by the subclass used. The type of a
        band does not necessarily affect how it is rastered, it mainly conveys
        useful information about the data it contains.
        *Filterable*.
    sort_order : int
        A number defining the default sort order for bands within a product. If not
        set for newly created bands, this will default to the current maximum sort
        order + 1 in the product.
        *Sortable*.
    data_type : DataType
        Required: The data type for pixel values in this band
    nodata : float
        A value representing missing data in a pixel in this band
    data_range : tuple(float, float)
        Required: The minimum and maximum pixel values stored in this band
    display_range : tuple(float, float)
        Required: A reasonable default range of pixel values when rastering
        this band for display purposes
    resolution : Resolution
        The spatial resolution of this band.
        *Filterable, sortable*.
    band_index : int
        Required: The 0-based index into the source data to access this band
    file_index : int
        The 0-based index into the list of source files, if there are multiple ones.
        Defaults to 0 (first file).
    jpx_layer_index : int
        The 0-based layer index if the source data is JPEG2000 with layers.
        Defaults to 0.
    """

    _doc_type = "band"
    _url = "/bands"
    _derived_type_switch = "type"
    _default_includes = ["product"]

    description = Attribute()
    type = EnumAttribute(BandType)
    sort_order = Attribute()
    data_type = EnumAttribute(DataType)
    nodata = Attribute()
    data_range = Attribute()
    display_range = Attribute()
    resolution = Resolution()
    band_index = Attribute()
    file_index = Attribute()
    jpx_layer_index = Attribute()

    def __new__(cls, *args, **kwargs):
        if cls is Band:
            raise TypeError(
                "Please instantiate one of the derived classes of 'Band' instead"
            )

        return super(Band, cls).__new__(cls)

    def __init__(self, **kwargs):
        if self._derived_type_switch not in kwargs:
            kwargs[self._derived_type_switch] = self._derived_type

        super(Band, self).__init__(**kwargs)

    @classmethod
    def search(cls, client=None):
        """A search query for all bands.

        Returns an instance of the
        :py:class:`~descarteslabs.catalog.search.Search` class configured for
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
        :py:class:`~descarteslabs.catalog.search.Search`
            An instance of the :py:class:`~descarteslabs.catalog.search.Search` class
        """
        search = super(Band, cls).search(client)
        if cls._derived_type:
            search = search.filter(properties.type == cls._derived_type)
        return search


class SpectralBand(Band):
    """A band that lies somewhere on the visible/NIR/SWIR electro-optical wavelength
    spectrum.

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
    base classes:

    * :py:class:`descarteslabs.catalog.Band`
    * :py:class:`descarteslabs.catalog.NamedCatalogObject`
    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    wavelength_nm_center : float
        Weighted center of min/max responsiveness of the band, in nm.
        *Filterable, sortable*.
    wavelength_nm_min : float
        Minimum wavelength this band is sensitive to, in nm.
        *Filterable, sortable*.
    wavelength_nm_max : float
        Maximum wavelength this band is sensitive to, in nm.
        *Filterable, sortable*.
    wavelength_nm_fwhm : float
        Full width at half maximum value of the wavelength spread, in nm.
        *Filterable, sortable*.
    """

    _derived_type = BandType.SPECTRAL.value

    wavelength_nm_center = Attribute()
    wavelength_nm_min = Attribute()
    wavelength_nm_max = Attribute()
    wavelength_nm_fwhm = Attribute()


class MicrowaveBand(Band):
    """A band that lies in the microwave spectrum, often from SAR or passive radar sensors.

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
    base classes:

    * :py:class:`descarteslabs.catalog.Band`
    * :py:class:`descarteslabs.catalog.NamedCatalogObject`
    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    frequency : float
        Center frequency of the observed microwave in GHz.
        *Filterable, sortable*.
    bandwidth : float
        Chirp bandwidth of the sensor in MHz.
        *Filterable, sortable*.
    """

    _derived_type = BandType.MICROWAVE.value

    frequency = Attribute()
    bandwidth = Attribute()


class MaskBand(Band):
    """A binary band where by convention a 0 means masked and 1 means non-masked.

    The :py:attr:`data_range` and :py:attr:`display_range` for masks is implicitly
    ``(0, 1)``.

    Parameters
    ----------
    kwargs : dict
        With the exception of readonly attributes
        (:py:attr:`~descarteslabs.catalog.CatalogObject.created`,
        :py:attr:`~descarteslabs.catalog.CatalogObject.modified`), and
        the computed attributes (`data_range`, `display_range`) any (inherited)
        attribute listed below can also be used as a keyword argument.

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base classes:

    * :py:class:`descarteslabs.catalog.Band`
    * :py:class:`descarteslabs.catalog.NamedCatalogObject`
    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    is_alpha : bool
        Whether this band should be useable as an alpha band during rastering.
        This enables special behavior for this band during rastering. If this
        is ``True`` and the band appears as the last band in a raster operation
        (such as :meth:`descarteslabs.scenes.scenecollection.SceneCollection.mosaic`
        or :meth:`descarteslabs.scenes.scenecollection.SceneCollection.stack`)
        pixels with a value of 0 in this band will be treated as transparent.
    data_range : tuple(float, float)
        Readonly: [0, 1].
    display_range : tuple(float, float)
        Readonly: [0, 1].
    """

    _derived_type = BandType.MASK.value

    is_alpha = Attribute()


class ClassBand(Band):
    """A band that maps a finite set of values that may not be continuous.

    For example land use classification.  A visualization with straight pixel values
    is typically not useful, so commonly a colormap is used.

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
    base classes:

    * :py:class:`descarteslabs.catalog.Band`
    * :py:class:`descarteslabs.catalog.NamedCatalogObject`
    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    colormap_name : str, Colormap
        Name of a predefined colormap for display purposes. The colormap is applied
        when this band is rastered by itself in PNG or TIFF format, including in
        UIs where imagery is visualized.
    colormap : list(tuple)
        A custom colormap for this band. A list of lists, where each nested list
        is a 4-tuple of RGBA values to map pixels whose value is the index of the
        tuple. E.g. the colormap ``[[100, 20, 200, 255]]`` would map pixels
        whose value is 0 in the original band to the RGBA color defined by
        ``[100, 20, 200, 255]``. The number of 4-tuples provided can be up
        to the maximum of this band's data range. Omitted values will map to black
        by default.
    class_labels : list(str or None)
        A list of labels where each element is a name for the class with the value at
        that index. Elements can be null if there is no label at that value.
    """

    _derived_type = BandType.CLASS.value

    colormap_name = EnumAttribute(Colormap)
    colormap = Attribute()
    class_labels = Attribute()


class GenericBand(Band):
    """A generic kind of band not fitting any other type.

    For example mapping physical values like temperature or angles.

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
    base classes:

    * :py:class:`descarteslabs.catalog.Band`
    * :py:class:`descarteslabs.catalog.NamedCatalogObject`
    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    physical_range : tuple(float, float)
        A physical range that pixel values map to
    physical_range_unit : str
        Unit of the physical range
    colormap_name : str, Colormap
        Name of a predefined colormap for display purposes. The colormap is applied
        when this band is rastered by itself in PNG or TIFF format, including in
        UIs where imagery is visualized.
    colormap : list(tuple)
        A custom colormap for this band. A list of lists, where each nested list
        is a 4-tuple of RGBA values to map pixels whose value is the index of the
        tuple. E.g. the colormap ``[[100, 20, 200, 255]]`` would map pixels
        whose value is 0 in the original band to the RGBA color defined by
        ``[100, 20, 200, 255]``. The number of 4-tuples provided can be up
        to the maximum of this band's data range. Omitted values will map to black
        by default.
    """

    _derived_type = BandType.GENERIC.value

    physical_range = Attribute()
    physical_range_unit = Attribute()
    colormap_name = EnumAttribute(Colormap)
    colormap = Attribute()


class DerivedBand(CatalogObject):
    """
    A type of band that is the result of a pixel function applied to one or more
    existing bands. This object type only supports read operations;
    they cannot be created, updated, or deleted using this client.

    Parameters
    ----------
    kwargs : dict
        This is a readonly object.

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base class:

    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    name : str
        Required, immutable: The name of the derived band, globally unique.
        *Filterable, sortable*.
    description : str
        Immutable: A description with further details on the derived band
    data_type : str
        Required, immutable: The data type for pixel values in this derived band
    data_range : tuple(float, float)
        Required, immutable: The minimum and maximum pixel values stored in
        this derived band
    physical_range : tuple(float, float)
        Immutable: A physical range that pixel values map to
    bands : list(str)
        Required, immutable: List of bands used in the derived band pixel function
        *Filterable*
    function_name : str
        Required, immutable: Name of the function applied to create this derived band

    Methods
    -------
    delete(ignore_missing=False)
        You cannot delete a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.

    """

    _doc_type = "derived_band"
    _url = "/derived_bands"

    name = Attribute()
    description = Attribute()
    data_type = EnumAttribute(DataType)
    data_range = Attribute()
    physical_range = Attribute()
    bands = Attribute()
    function_name = Attribute()

    def save(self):
        """You cannot save a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.
        """
        raise NotImplementedError("Saving and updating DerivedBands is not permitted")

    @classmethod
    def delete(cls, id, client=None, ignore_missing=False):
        """You cannot delete a derived band.

        Raises
        ------
        NotImplementedError
            This method is not supported for DerivedBands.
        """
        raise NotImplementedError("Deleting DerivedBands is not permitted")

    def _instance_delete(self, ignore_missing=False):
        raise NotImplementedError("Deleting DerivedBands is not permitted")
