from typing import Optional
import operator

import traitlets
import ipywidgets


from ..types import Image, ImageCollection, Proxytype
from .layer import WorkflowsLayer


class LayerPicker(ipywidgets.HBox):
    """
    Widget to pick a WorkflowsLayer from a map

    In subclasses, set `_attr` to the trait name on WorkflowsLayer that you want mirrored into
    the `value` trait of this class.

    Attributes
    ----------
    value: ImageCollection, None
        The parametrized ImageCollection of the currently-selected layer.
    """

    value = traitlets.Instance(klass=ImageCollection, allow_none=True, read_only=True)
    _attr = "value"

    def __init__(
        self,
        map=None,
        default_layer: Optional[WorkflowsLayer] = None,
        hide_deps_of: Optional[Proxytype] = None,
        **kwargs,
    ):
        """
        Construct a LayerPicker widget for a map.

        Parameters
        ----------
        map: ipyleaflet.Map
            The map instance to pick from. Defaults to `wf.map`.
        default_layer: WorkflowsLayer
            The layer instance to have selected by default
        hide_deps_of: Proxytype
            Hide any layers from the dropdown that have this object in their ``.params``.
            Mainly used by the Picker parameter widget to hide its own layer from the dropdown,
            avoiding graft cycles.
        """
        super().__init__(**kwargs)
        if map is None:
            # use wf.map as default
            from . import map

        # awkwardly handle MapApp without circularly importing it for an isinstance check
        try:
            map = map.map
        except AttributeError:
            pass

        self._map = map
        self._hide_deps_of = hide_deps_of
        self._dropdown = ipywidgets.Dropdown(equals=operator.is_)

        type_ = type(self).value.klass
        if default_layer is not None:
            if not isinstance(default_layer, WorkflowsLayer):
                raise TypeError(
                    f"Default values for an {type(self).__name__} can only be WorkflowsLayer instances "
                    f"(the layer object returned by `.visualize`), not {default_layer!r}."
                    "Also note that this default value won't be synced when publishing."
                )
            value = getattr(default_layer, self._attr)
            if not isinstance(value, type_):
                raise TypeError(
                    f"Expected a default layer visualizing an {type_.name}, not an {type(value).__name__}. "
                    "Pick a different layer, or pick a different type for this widget, or remove a "
                    "reduction operation (like `.mosaic()`, `.mean('images')`) from the code that "
                    f"produces the layer {default_layer.name!r}"
                )
            self.set_trait("value", value)
            default_layer.observe(self._picked_layer_value_changes, self._attr)
        self._picked_layer = default_layer

        map.observe(self._update_options, "layers")
        self._dropdown.observe(self._layer_picked, "value")

        self.children = [self._dropdown]
        self._setting_options = False

        self._update_options({})

    def _update_options(self, change):
        type_ = type(self).value.klass
        options = [
            (lyr.name, lyr)
            for lyr in reversed(self._map.layers)
            if isinstance(lyr, WorkflowsLayer)
            and isinstance(getattr(lyr, self._attr), type_)
            and all(p is not self._hide_deps_of for p in lyr.xyz_obj.params)
        ]

        # when changing options, ipywidgets always just picks the first option.
        # this is infuriatingly difficult to work around, so we set our own flag to ignore
        # changes while this is happening.
        self._setting_options = True
        self._dropdown.options = options
        self._setting_options = False

        try:
            self._dropdown.value = self._picked_layer
        except traitlets.TraitError:
            # the previously-picked layer doesn't exist anymore;
            # we'd rather just have no value in that case
            self._dropdown.value = None
            self._picked_layer = None
            self.set_trait("value", None)

    def _layer_picked(self, change):
        new_layer = change["new"]
        if self._setting_options or new_layer is self._picked_layer:
            return

        if self._picked_layer is not None:
            self._picked_layer.unobserve(self._picked_layer_value_changes, self._attr)

        if new_layer is None:
            self._picked_layer = None
            self.set_trait("value", None)
        else:
            new_layer.observe(self._picked_layer_value_changes, self._attr)
            self._picked_layer = new_layer
            self.set_trait("value", getattr(new_layer, self._attr))

    def unlink(self):
        self._map.unobserve("layers", self._update_options)
        self._dropdown.unobserve("value", self._layer_picked)
        if self._picked_layer is not None:
            self._picked_layer.unobserve(self._picked_layer_value_changes, self._attr)
        self._picked_layer = None

    def _picked_layer_value_changes(self, change):
        self.set_trait("value", change["new"])

    def _ipython_display_(self):
        super()._ipython_display_()


class ImagePickerWidget(LayerPicker):
    """
    Widget to pick a layer from the map, as an Image.

    If selecting an ImageCollection layer, this gives it with its reduction applied.

    Note you cannot change the selected layer programmatically,
    only by using the widget.
    """

    value = traitlets.Instance(klass=Image, allow_none=True, read_only=True)
    _attr = "image_value"


class ImageCollectionPickerWidget(LayerPicker):
    """
    Widget to pick a layer from the map, as an ImageCollection.

    Only layers showing ImageCollections (not Images) will be shown.

    Note you cannot change the selected layer programmatically,
    only by using the widget.
    """
