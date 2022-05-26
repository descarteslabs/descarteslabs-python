from . import widgets

try:
    from .layer import WorkflowsLayer
    from .layer_controller import LayerController, LayerControllerList
    from .layer_picker import ImagePickerWidget, ImageCollectionPickerWidget
    from .map_ import MapApp, Map
    from .parameters import ParameterSet, ProxytypeInstance
    from .inspector import PixelInspector
    from .workflow import WorkflowsBrowser

    map = MapApp()
    """
    A single `MapApp` instance that all ``visualize`` calls are automatically
    added to. This is the best starting point for using Workflows interactive maps.

    Run::

        wf.map

    in a JupyterLab cell to display the map the first time, then right-click and select "New View for Output".
    You can then position and rearrange the map as a tab however you like.

    Calling `.Image.visualize` will by default add new layers to this map.
    """

    flows = WorkflowsBrowser()
    """
    Browse shared `Workflow` objects (and add them to the map) with a predefined `WorkflowsBrowser` instance.
    """

    _successful_import = True
except Exception as e:
    _successful_import = False
    error = e

    class MissingImports(object):
        def _error(self, *args, **kwargs):
            err = ImportError(
                "Optional dependencies needed for map visualization and Workflow browsing "
                "are missing or outdated.\n"
                "Please install `ipyleaflet` and `traitlets`, or better:\n"
                "$ pip install --upgrade 'descarteslabs[complete]'\n"
                "Then, install the Jupyter extensions:\n"
                "$ jupyter labextension install jupyter-leaflet @jupyter-widgets/jupyterlab-manager\n"
                "$ jupyter nbextension enable --py --sys-prefix ipyleaflet\n"
                "Finally, restart the kernel, refresh the webpage, and re-run the notebook."
            )
            raise err from error

        __getattr__ = __getitem__ = __call__ = __dir__ = __repr__ = _error

    map = MissingImports()
    flows = map
    WorkflowsLayer = map
    LayerController = map
    LayerControllerList = map
    ImagePickerWidget = map
    ImageCollectionPickerWidget = map
    MapApp = map
    Map = map
    ParameterSet = map
    ProxytypeInstance = map
    PixelInspector = map
    WorkflowsBrowser = map


__all__ = [
    "WorkflowsLayer",
    "LayerController",
    "LayerControllerList",
    "ImagePickerWidget",
    "ImageCollectionPickerWidget",
    "Map",
    "MapApp",
    "map",
    "ParameterSet",
    "ProxytypeInstance",
    "PixelInspector",
    "WorkflowsBrowser",
    "flows",
    "widgets",
]
