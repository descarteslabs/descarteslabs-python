try:
    from ipyleaflet import VectorTileLayer
    from traitlets import Dict
except ImportError:
    VectorTileLayer = object


class DLVectorTileLayer(VectorTileLayer):
    """
    A minimal wrapper around VectorTileLayer to add fetch_options
    """

    def __init__(self, *args, **kwargs):
        if VectorTileLayer is object:
            raise ImportError(
                "The visualization support in the `descarteslabs.vector` Python package"
                " requires the `viz` extra to be installed."
                " Please run `pip install descarteslabs[viz]` and try again."
            )
        super(DLVectorTileLayer, self).__init__(*args, **kwargs)
        self.fetch_options = Dict({"credentials": "include"}).tag(sync=True, o=True)
