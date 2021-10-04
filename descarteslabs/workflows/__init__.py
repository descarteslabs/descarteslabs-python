from .types import (
    Proxytype,
    ProxyTypeError,
    proxify,
    Function,
    Primitive,
    Number,
    Int,
    Float,
    Bool,
    Str,
    NoneType,
    Any,
    Array,
    MaskedArray,
    Tuple,
    List,
    Dict,
    Struct,
    range,
    zip,
    Datetime,
    Timedelta,
    Feature,
    FeatureCollection,
    load_geojson,
    load_geojson_file,
    Geometry,
    GeometryCollection,
    GeoContext,
    Image,
    ImageCollection,
    Kernel,
    ImageCollectionGroupby,
    log,
    log2,
    log10,
    log1p,
    sqrt,
    cos,
    arccos,
    sin,
    arcsin,
    tan,
    arctan,
    arctan2,
    exp,
    square,
    normalized_difference,
    concat,
    where,
    conv2d,
    ifelse,
    e,
    inf,
    nan,
    pi,
    parameter,
    numpy,
)

from .models import (
    JobComputeError,
    JobTimeoutError,
    VizOption,
    Workflow,
    VersionedGraft,
    Job,
    XYZ,
    XYZLogListener,
    compute,
    use,
    publish,
    wmts_url,
)

from .inspect import (
    inspect,
    InspectClient,
)

from .interactive import (
    map,
    flows,
    widgets,
    Map,
    WorkflowsLayer,
    LayerController,
    LayerControllerList,
)

from ._channel import __channel__, _set_channel
from .client import Client, exceptions


__all__ = [
    # .types
    "Proxytype",
    "ProxyTypeError",
    "parameter",
    "proxify",
    "Function",
    "Primitive",
    "Number",
    "Int",
    "Float",
    "Bool",
    "Str",
    "NoneType",
    "Any",
    "Array",
    "MaskedArray",
    "Tuple",
    "List",
    "Dict",
    "Struct",
    "range",
    "zip",
    "Datetime",
    "Timedelta",
    "Feature",
    "FeatureCollection",
    "load_geojson",
    "load_geojson_file",
    "Geometry",
    "GeometryCollection",
    "GeoContext",
    "Image",
    "ImageCollection",
    "Kernel",
    "ImageCollectionGroupby",
    "log",
    "log2",
    "log10",
    "log1p",
    "sqrt",
    "cos",
    "arccos",
    "sin",
    "arcsin",
    "tan",
    "arctan",
    "arctan2",
    "exp",
    "square",
    "normalized_difference",
    "concat",
    "where",
    "conv2d",
    "ifelse",
    "e",
    "inf",
    "nan",
    "pi",
    # .models
    "JobComputeError",
    "JobTimeoutError",
    "VizOption",
    "Workflow",
    "VersionedGraft",
    "Job",
    "XYZ",
    "XYZLogListener",
    "use",
    # ._channel
    "__channel__",
    "_set_channel",
    # .client
    "Client",
    "exceptions",
    # __init__
    "compute",
    "publish",
    "wmts_url",
    # .inspect
    "inspect",
    "InspectClient",
    # .interactive
    "map",
    "flows",
    "widgets",
    "Map",
    "WorkflowsLayer",
    "LayerController",
    "LayerControllerList",
    # .numpy
    "numpy",
]

# Add `.compute`, `.publish`, `.inspect` wrapper methods to Proxytype instances.

# NOTE(gabe): we monkey-patch these `.compute` and `.publish` methods onto the base
# Proxytype class here, rather than adding them to Proxytype directly in ``types/core/core.py``,
# because they create too much circular dependency between `models` and `types`.
# We feel this is the most reasonable approach because:
# * The circular dependencies get ridiculous otherwise (the abstract base Proxytype class
#   depends on the `GeoContext` concrete subtype!)
# * They are purely user-facing convenience methods; none of the types know,
#   care about, or rely on having a `compute` method.
# * __init__ is the place that brings these two submodules together,
#   thus it's also the right place for the helper methods that unify these modules.


def _remove_doc_parameter(doc: str, param: str, indent: str = " " * 4) -> str:
    "remove the block for the given argument name from a docstring"
    import re

    return re.sub(
        f"{indent}{param}.+?\n{indent}(?=\\w)", indent, doc, count=1, flags=re.DOTALL
    )


def _remove_doc_examples(doc: str, indent: str = " " * 4) -> str:
    "remove the Examples section from a docstring. must be the last section."
    main, heading, example = doc.partition(f"\n{indent}Examples\n")
    return main + "\n"


def _signature_without_params(func, *names):
    "the signature of a function, with given parameter names removed"
    import inspect

    sig = inspect.signature(func)
    return sig.replace(
        parameters=[p for name, p in sig.parameters.items() if name not in names]
    )


def _compute_mixin(self, *args, **kwargs):
    return compute(self, *args, **kwargs)


def _publish_mixin(self, id, version, *args, **kwargs):
    return publish(id, version, self, *args, **kwargs)


def _inspect_mixin(self, *args, **kwargs):
    return inspect(self, *args, **kwargs)


_compute_mixin.__signature__ = _signature_without_params(compute, "obj")
_publish_mixin.__signature__ = _signature_without_params(publish, "obj")
_inspect_mixin.__signature__ = _signature_without_params(inspect, "obj")

_compute_mixin.__doc__ = _remove_doc_examples(
    _remove_doc_parameter(compute.__doc__, "obj")
)
_publish_mixin.__doc__ = _remove_doc_examples(
    _remove_doc_parameter(
        publish.__doc__.replace(" Can also be used as a decorator.", "", 1).replace(
            "\n        If used as a decorator, returns the `~.Function` instead.",
            "",
            1,
        ),
        "obj",
    )
)
_inspect_mixin.__doc__ = _remove_doc_parameter(inspect.__doc__, "obj")

_compute_mixin.__name__ = "compute"
_publish_mixin.__name__ = "publish"
_inspect_mixin.__name__ = "inspect"

Proxytype.compute = _compute_mixin
Proxytype.publish = _publish_mixin
Proxytype.inspect = _inspect_mixin
