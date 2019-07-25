import six


try:
    from .layer import WorkflowsLayer
    from .layer_controller import LayerController, LayerControllerList
    from .map_ import MapApp, Map

    map = MapApp()
    """
    A single `MapApp` instance that all ``visualize`` calls are automatically added to.

    Run::

        wf.map

    in a JupyterLab cell to display the map the first time, then right-click and select "New View for Output".
    You can then position and rearrange the map as a tab however you like.

    Calling `.Image.visualize` will by default add new layers to this map.
    """

except Exception as e:
    error = e

    class MissingImports(object):
        def _error(self, *args, **kwargs):
            err = ImportError(
                "Missing optional dependencies needed for map visualization. "
                "Please install `ipyleaflet`, or better, `pip install --upgrade 'descarteslabs[complete]'` and rerun."
            )
            six.raise_from(err, error)

        __getattr__ = __getitem__ = __call__ = __dir__ = _error

    map = MissingImports()
    WorkflowsLayer = map
    LayerController = map
    LayerControllerList = map
    MapApp = map
    Map = map


__all__ = [
    "WorkflowsLayer",
    "LayerController",
    "LayerControllerList",
    "Map",
    "MapApp",
    "map",
]
