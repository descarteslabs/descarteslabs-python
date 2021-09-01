import six
from enum import Enum

try:
    from collections import abc
except ImportError:
    import collections as abc

# supported data types.
# values must be ordered from smallest to largest, and
# (arbitrarily) unsigned before signed, integer before float
valid_data_types = ("Byte", "UInt16", "Int16", "UInt32", "Int32", "Float32", "Float64")

# supported upcasts for each data type
valid_data_type_casts = {
    "Byte": ("UInt16", "Int16", "UInt32", "Int32", "Float32", "Float64"),
    "UInt16": ("UInt32", "Int32", "Float32", "Float64"),
    "Int16": ("Int32", "Float32", "Float64"),
    "UInt32": ("Float64"),
    "Int32": ("Float32", "Float64"),
    "Float32": ("Float64"),
    "Float64": (),
}


# min/max ranges for supported data types
data_type_ranges = {
    "Byte": [0, 255],
    "UInt16": [0, 65535],
    "Int16": [-32768, 32767],
    "UInt32": [0, 4294967295],
    "Int32": [-2147483648, 2147483647],
    # for floats, this is our default output range,
    # and does not imply the range a float can contain.
    "Float32": [0.0, 1.0],
    "Float64": [0.0, 1.0],
}


class ScalingMode(Enum):
    RAW = "raw"
    DISPLAY = "display"
    AUTO = "auto"
    PHYSICAL = "physical"


class BandType(Enum):
    SPECTRAL = "spectral"
    MASK = "mask"
    CLASS = "class"
    OTHER = "other"
    # v1 compatibility
    DERIVED = "other"


class BandScale(object):
    # An explanation for the `mode` and `mode_is_implied` properties of BandScales:
    #
    # A non-implied mode (`mode_is_implied=False`) is when the user has explicitly
    # stated a mode, e.g. "display", "physical", etc. If two bands specify different
    # incompatible modes, this is an error on the part of the user (however "auto"
    # and "display" are fungible). Thus you see `check_mode` raising an error.
    #
    # An implied mode (`mode_is_implied=True` is when the user has not explicitly
    # stated a mode, and instead we are inferring it from the tuple. This is guesswork,
    # and if two different bands imply two incompatible modes, it is not an error but
    # rather a case in which we cannot determine a mode.
    #
    # Both are used in order to determine which possible input range to use (`default_range` or
    # `data_range`), which possible output range to use (range of the data type, `physical_range`,
    # or [0, 255]), and how to default the output data type. AN explicit mode is stronger, and
    # the implied mode is never consulted unless there is no explicit mode.

    def __init__(self, name, properties, mode, mode_is_implied):
        """
        Construct a BandScale instance from band properties and scale specification.

        Will raise ValueError for any bad input.
        """
        self.name = name
        self._properties = {
            k: properties.get(k)
            for k in ("type", "dtype", "data_range", "default_range", "physical_range")
            if k in properties
        }
        # ensure required fields are available:
        if "type" not in self._properties:
            # handle a derived band
            if name.startswith("derived:"):
                self._properties["type"] = BandType.OTHER.value
            else:
                raise ValueError(
                    "Invalid properties for band '{}' is missing 'type' field".format(
                        name
                    )
                )
        if "dtype" not in self._properties:
            raise ValueError(
                "Invalid properties for band '{}' is missing 'dtype' field".format(name)
            )
        if "data_range" not in self._properties:
            raise ValueError(
                "Invalid properties for band '{}' is missing 'data_range' field".format(
                    name
                )
            )
        # default_range isn't always populated
        if "default_range" not in self._properties:
            self._properties["default_range"] = self._properties["data_range"]
        # physical_range isn't populated for mask and class types
        if "physical_range" not in self._properties:
            self._properties["physical_range"] = self._properties["data_range"]
        self.mode = mode
        self.mode_is_implied = mode_is_implied

    def __getattr__(self, attr):
        if attr in self._properties:
            return self._properties[attr]
        raise AttributeError(
            "{} object has no '{}' attribute".format(self.__class__.__name__, attr)
        )

    def output_range(self):
        raise NotImplementedError

    def get_scale(self, mode, data_type):
        raise NotImplementedError


class NoBandScale(BandScale):
    def __init__(self, name, properties, mode=None):
        super(NoBandScale, self).__init__(name, properties, mode, True)

    def output_range(self):
        return self.data_range

    def get_scale(self, mode, data_type):
        return None


class AutomaticBandScale(BandScale):
    def __init__(self, name, properties, mode):
        super(AutomaticBandScale, self).__init__(name, properties, mode, False)

    def output_range(self):
        if self.mode == ScalingMode.RAW:
            return self.data_range
        elif self.mode == ScalingMode.PHYSICAL:
            return self.physical_range
        else:
            return [0, 255]

    def get_scale(self, mode, data_type):
        ofloat = data_type.startswith("Float")
        if self.mode == ScalingMode.RAW:
            return None
        elif self.mode == ScalingMode.AUTO:
            # this is handled by GDAL itself
            return ()
        elif self.mode == ScalingMode.DISPLAY:
            # 255.99 from GDAL
            return tuple(self.default_range + [0, 255.99 if ofloat else 255])
        elif self.mode == ScalingMode.PHYSICAL:
            return tuple(self.data_range + self.physical_range)


class TupleBandScale(BandScale):
    def __init__(self, name, properties, value):
        is_pct = []
        is_float = []
        for t in value:
            if isinstance(t, six.string_types):
                if not t.endswith("%"):
                    raise ValueError(
                        "Invalid scaling tuple value '{}' for band '{}' is not a percentage string".format(
                            t, name
                        )
                    )
                try:
                    float(t[:-1])
                except ValueError:
                    raise ValueError(
                        "Invalid scaling tuple value '{}' for band '{}' is not a percentage string".format(
                            t, name
                        )
                    )
                is_pct.append(True)
                is_float.append(False)
            elif isinstance(t, int):
                is_pct.append(False)
                is_float.append(True)
            elif isinstance(t, float):
                is_pct.append(False)
                is_float.append(True)
            else:
                raise ValueError(
                    "Invalid scaling value {} for band '{}' is not a number".format(
                        t, name
                    )
                )
        if len(value) == 0:
            mode = ScalingMode.AUTO
        elif len(value) == 2:
            mode = ScalingMode.DISPLAY
        elif is_float[2] or is_float[3]:
            mode = ScalingMode.PHYSICAL
        elif (not is_pct[2] and value[2] < 0) or (not is_pct[3] and value[3] > 255):
            mode = ScalingMode.RAW
        else:
            mode = None
        super(TupleBandScale, self).__init__(name, properties, mode, True)
        self._tuple = value
        self._is_pct = is_pct

    def __eq__(self, other):
        return super(TupleBandScale, self).__eq__(other) and self._tuple == other._tuple

    def output_range(self):
        if len(self._tuple) == 4:
            return [
                None if self._is_pct[2] else self._tuple[2],
                None if self._is_pct[3] else self._tuple[3],
            ]
        else:
            return [0, 255]

    def get_scale(self, mode, data_type):
        ifloat = self.dtype.startswith("Float")
        ofloat = data_type.startswith("Float")
        if len(self._tuple) == 0:
            # GDAL handles this
            return ()
        else:
            # generate default ranges
            if mode == ScalingMode.RAW:
                irange = data_type_ranges[self.dtype]
                # not sure about this; GDAL always uses 0, 255
                orange = data_type_ranges[data_type]
            elif mode == ScalingMode.PHYSICAL:
                irange = self.data_range
                orange = self.physical_range
            else:
                irange = self.default_range
                orange = [0.0, 255.99]  # from GDAL, works for integer also
            if len(self._tuple) == 2:
                scale = (self._tuple[0], self._tuple[1], orange[0], orange[1])
            else:
                scale = self._tuple
            # apply any percentage calculations across the tuple
            return tuple(
                map(
                    calc_pct,
                    scale,
                    (irange, irange, orange, orange),
                    (ifloat, ifloat, ofloat, ofloat),
                )
            )


def make_band_scale(name, properties, value):
    """
    Create a BandScale instance appropriate for the scale value.

    Raises ValueError on invalid input.
    """
    if value is None:
        return NoBandScale(name, properties)
    elif isinstance(value, six.string_types):
        try:
            mode = ScalingMode(value)
        except ValueError:
            raise ValueError(
                "Invalid scaling mode '{}' for band '{}'".format(value, name)
            )
        if name.startswith("derived:"):
            band_type = BandType.OTHER
        else:
            try:
                band_type = BandType(properties["type"])
            except ValueError:
                raise ValueError(
                    "Invalid band type '{}' for band '{}'".format(
                        properties["type"], name
                    )
                )
        if band_type in (BandType.MASK, BandType.CLASS):
            # do not scale these automatically, and make mode weak default
            return NoBandScale(name, properties, mode)
        else:
            return AutomaticBandScale(name, properties, mode)
    elif isinstance(value, (list, tuple)):
        if len(value) not in (0, 2, 4):
            raise ValueError(
                "Invalid scaling tuple {} for band '{}'".format(value, name)
            )
        return TupleBandScale(name, properties, tuple(value))


def parse_scaling(properties, bands, scaling):
    """
    Parse the scaling parameter, returning a list of BandScale instances.

    Properties should be a dictionary by band name of band properties.

    Will raise ValueError for any invalid input.
    """
    scales = []
    # handle four types of values permitted for scaling parameter
    if scaling is None:
        # no scaling
        scales = None
    elif isinstance(scaling, six.string_types):
        # automatic mode for all
        for band in bands:
            scales.append(make_band_scale(band, properties[band], scaling))
    elif isinstance(scaling, abc.Mapping):
        # dictionary like mapping for looking up bands and band types
        for band in bands:
            # None is an allowed value, so don't use get()
            if band in scaling:
                bscale = scaling[band]
            else:
                try:
                    band_type = BandType(properties[band]["type"])
                except ValueError:
                    raise ValueError(
                        "Invalid band type '{}' for band '{}'".format(band_type, band)
                    )
                if band_type in (BandType.MASK, BandType.CLASS):
                    bscale = None
                else:
                    bscale = scaling.get("default_", None)
            scales.append(make_band_scale(band, properties[band], bscale))
    elif isinstance(scaling, abc.Iterable):
        # list, tuple, etc.
        for i, bscale in enumerate(scaling):
            if i >= len(bands):
                raise ValueError("Invalid scaling value has more elements than bands")
            band = bands[i]
            scales.append(make_band_scale(band, properties[band], bscale))
        if i + 1 < len(bands):
            raise ValueError("Invalid scaling value has fewer elements than bands")
    else:
        raise ValueError(
            "Invalid scaling value {} has unsupported type".format(scaling)
        )

    return scales


def append_alpha_scaling(scaling):
    """
    If the scaling parameter is an iterable (list), add a
    None on the end to match an alpha band which has been
    added to the end of the band list by the caller.
    """
    if (
        scaling is None
        or isinstance(scaling, six.string_types)
        or isinstance(scaling, abc.Mapping)
        or not isinstance(scaling, abc.Iterable)
    ):
        return scaling
    return list(scaling) + [None]


def common_data_type(data_types):
    """
    Return the common (GDAL) data type that all of the given data types can be cast to, or
    None if there is no common one.
    """
    if len(data_types) == 0:
        return None
    elif len(data_types) == 1:
        if data_types[0] not in valid_data_types:
            raise ValueError("Invalid data type '{}'".format(data_types[0]))
        return data_types[0]
    else:
        dtype1 = common_data_type(data_types[0:-1])
        if dtype1 is None:
            return None
        types1 = valid_data_type_casts[dtype1]
        dtype2 = data_types[-1]
        if dtype2 not in valid_data_types:
            raise ValueError("Invalid data type '{}'".format(dtype2))
        types2 = valid_data_type_casts[dtype2]
        dtype = None

        if dtype1 == dtype2 or dtype1 in types2:
            dtype = dtype1
        elif dtype2 in types1:
            dtype = dtype2
        else:
            for dt in types2:
                if dt in types1:
                    dtype = dt
                    break

        return dtype


def data_type_from_range(min, max, is_float):
    """
    Return the GDAL data type which can represent the provided min and max values.
    """
    # short circuit float since range is 0-1
    if (
        is_float
        or (min is not None and min < data_type_ranges["Int32"][0])
        or (max is not None and max > data_type_ranges["Int32"][1])
    ):
        return "Float64"
    for dt in valid_data_types:
        rmin, rmax = data_type_ranges[dt]
        if (min is None or min >= rmin) and (max is not None and max <= rmax):
            return dt
    return "Float64"


def calc_pct(value, bounds, is_float):
    """
    Helper function to calculate a scaling tuple value from a percentage.
    """
    if isinstance(value, six.string_types):
        value = float(value[:-1]) * (bounds[1] - bounds[0]) / 100.0 + bounds[0]
    return value if is_float else int(value)


def check_modes(scales, implied=False):
    """
    Checks for and returns any combined mode settings.

    If implied=False, checks for explicit modes and errors on conflict.
    Otherwise checks for implicit modes and returns None on conflict.

    Raises a ValueError if there is any conflict.
    """
    mode = None
    for bscale in scales:
        if bscale.mode is not None and bscale.mode_is_implied == implied:
            if mode is None:
                mode = bscale.mode
            elif bscale.mode != mode:
                if bscale.mode in (ScalingMode.AUTO, ScalingMode.DISPLAY) and mode in (
                    ScalingMode.AUTO,
                    ScalingMode.DISPLAY,
                ):  # noqa
                    mode = ScalingMode.DISPLAY
                elif implied:
                    # cannot determine implied mode on conflict
                    return None
                else:
                    raise ValueError(
                        "Conflicting modes in scaling: '{}' and '{}'".format(
                            mode.value, bscale.mode.value
                        )
                    )
    return mode


def check_implied_data_type(scales):
    """
    Using any supplied output ranges, finds an output data type which
    can hold everything.

    Returns None only when all scales are 4-tuples with percentage
    values for the output range.
    """
    # this is a "transpose" from a list or ranges to a list of mins and maxes
    ranges = list(zip(*[bscale.output_range() for bscale in scales]))
    # filter out None values
    min_list = list(filter(lambda x: x is not None, ranges[0]))
    max_list = list(filter(lambda x: x is not None, ranges[1]))
    is_float = any(map(lambda x: isinstance(x, float), min_list + max_list))
    output_min = min(min_list) if min_list else None
    output_max = max(max_list) if max_list else None
    if output_min is None and output_max is None:
        return None
    return data_type_from_range(output_min, output_max, is_float)


def scaling_parameters(properties, bands, scaling, data_type):
    """
    Determine GDAL-style band scaling parameters.

    properties is the "bands" dictionary from a product.

    returns scales, data_type where scales is either a None or a list of tuples and Nones,
    and data_type is the target GDAL data type.
    """
    # validate bands
    for band in bands:
        if band not in properties:
            message = "Invalid bands: band '{}' is not available".format(band)
            if "derived:{}".format(band) in properties:
                message += ", did you mean 'derived:{}'?".format(band)
            raise ValueError(message)

    # validate data_type
    if data_type is not None and data_type not in valid_data_types:
        raise ValueError("Invalid data_type value {}")

    # handle this common case quickly
    if scaling is None:
        if data_type is None:
            data_type = common_data_type([properties[band]["dtype"] for band in bands])
        return scaling, data_type

    # get list of BandScales, validates scaling possibly throwing ValueError
    scales = parse_scaling(properties, bands, scaling)

    # at this point everything is validated, except possible conflicts between
    # specifications for individual bands.

    # check any explicit modes, will raise error on conflict
    mode = check_modes(scales)

    # time to do some guesswork from tuples
    if mode is None and data_type is None:
        mode = check_modes(scales, implied=True)
        if mode is None:
            data_type = check_implied_data_type(scales)
            if data_type is None:
                # we have nothing to go on!
                raise ValueError(
                    "Invalid scaling parameters, cannot determine output data type or mode"
                )

    # at least one of mode or data_type is not None
    # default one from the other if needed
    if mode is None:
        if data_type == "Byte":
            mode = ScalingMode.DISPLAY
        elif data_type == "Float32" or data_type == "Float64":
            mode = ScalingMode.PHYSICAL
        else:
            mode = ScalingMode.RAW
    elif data_type is None:
        if mode == ScalingMode.RAW:
            data_type = common_data_type([properties[band]["dtype"] for band in bands])
        elif mode == ScalingMode.PHYSICAL:
            data_type = "Float64"
        else:
            data_type = "Byte"

    # now take a pass to determine complete scaling for each band
    scales = [bscale.get_scale(mode, data_type) for bscale in scales]

    return scales, data_type


def multiproduct_scaling_parameters(properties, bands, scaling, data_type):
    """
    Determine GDAL-style band scaling parameters.

    properties is the dictionary keyed by the product id with values of the "bands" dictionary from that product.
    bands must already be validated.

    returns scales, data_type where scales is either a None or a list of tuples and Nones,
    and data_type is the target GDAL data type.
    """
    # validate bands
    for band in bands:
        for product in properties:
            if band not in properties[product]:
                message = (
                    "Invalid bands: band '{}' is not available in product '{}'".format(
                        band, product
                    )
                )
                if "derived:{}".format(band) in properties[product]:
                    message += ", did you mean 'derived:{}'?".format(band)
                raise ValueError(message)

    # validate data_type
    if data_type is not None and data_type not in valid_data_types:
        raise ValueError("Invalid data_type value {}")

    # handle this common case quickly
    if scaling is None:
        if data_type is None:
            data_type = common_data_type(
                [
                    properties[product][band]["dtype"]
                    for band in bands
                    for product in properties
                ]
            )
        return scaling, data_type

    # loop over all products and bands, get list of BandScales, validates scaling possibly throwing ValueError
    scales = []
    for product in properties:
        scales.extend(parse_scaling(properties[product], bands, scaling))

    # at this point everything is validated, except possible conflicts between
    # specifications for individual bands. This will be checked below.

    # check any explicit modes, will raise error on conflict
    mode = check_modes(scales)

    # time to do some guesswork from tuples
    if mode is None and data_type is None:
        mode = check_modes(scales, implied=True)
        if mode is None:
            data_type = check_implied_data_type(scales)
            if data_type is None:
                # we have nothing to go on!
                raise ValueError(
                    "Invalid scaling parameters, cannot determine output data type or mode"
                )

    # at least one of mode or data_type is not None
    # default one from the other if needed
    if mode is None:
        if data_type == "Byte":
            mode = ScalingMode.DISPLAY
        elif data_type == "Float32" or data_type == "Float64":
            mode = ScalingMode.PHYSICAL
        else:
            mode = ScalingMode.RAW
    elif data_type is None:
        if mode == ScalingMode.RAW:
            data_type = common_data_type(
                [
                    properties[product][band]["dtype"]
                    for band in bands
                    for product in properties
                ]
            )
        elif mode == ScalingMode.PHYSICAL:
            data_type = "Float64"
        else:
            data_type = "Byte"

    # now take a pass to determine complete scaling for each product*band
    scales = [bscale.get_scale(mode, data_type) for bscale in scales]

    # verify the resulting scale parameters for each band is the same across
    # all products
    products = [product for product in properties]
    for i in range(1, len(properties)):
        for j in range(len(bands)):
            if scales[i * len(bands) + j] != scales[j]:
                raise ValueError(
                    "Invalid scaling incompatible bands for band '{}' in products '{}' and '{}'".format(
                        bands[i], products[0], products[i]
                    )
                )

    scales = scales[0 : len(bands)]

    return scales, data_type
