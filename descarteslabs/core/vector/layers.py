# Copyright 2018-2024 Descartes Labs.

try:
    from ipyleaflet import VectorTileLayer
    from traitlets import Dict
except ImportError:
    raise ImportError(
        "The visualization support in the `descarteslabs.vector` Python package"
        " requires the `viz` extra to be installed."
        " Please run `pip install descarteslabs[viz]` and try again."
        " Alternatively you can install the `ipyleaflet` package directly."
    )


class DLVectorTileLayer(VectorTileLayer):
    """
    A minimal wrapper around VectorTileLayer to add fetch_options
    """

    fetch_options = Dict({"credentials": "include"}).tag(sync=True, o=True)
