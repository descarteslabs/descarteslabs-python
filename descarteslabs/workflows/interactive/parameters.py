import copy
import datetime
import contextlib

import six
import traitlets
import ipywidgets


from .. import types


# Copy the old implementation of `link` from traitlets 4.3.3.
# traitlets >= 5.0.0 introduce a `!=` comparison when updating,
# which fails on types that overload equality checking (like NumPy arrays, Images, etc.).
# https://github.com/ipython/traitlets/blob/5.0.0/traitlets/traitlets.py#L296

# For our use here, the simpler older version works fine.

# BEGIN copied from
# https://github.com/ipython/traitlets/blob/4.3.3/traitlets/traitlets.py#L243-L302


def _validate_link(*tuples):
    """Validate arguments for traitlet link functions"""
    for t in tuples:
        if not len(t) == 2:
            raise TypeError(
                "Each linked traitlet must be specified as (HasTraits, 'trait_name'), not %r"
                % t
            )
        obj, trait_name = t
        if not isinstance(obj, traitlets.HasTraits):
            raise TypeError("Each object must be HasTraits, not %r" % type(obj))
        if trait_name not in obj.traits():
            raise TypeError("%r has no trait %r" % (obj, trait_name))


class SimpleLink:
    """Link traits from different objects together so they remain in sync.
    Parameters
    ----------
    source : (object / attribute name) pair
    target : (object / attribute name) pair
    """

    updating = False

    def __init__(self, source, target):
        _validate_link(source, target)
        self.source, self.target = source, target
        try:
            setattr(target[0], target[1], getattr(source[0], source[1]))
        finally:
            source[0].observe(self._update_target, names=source[1])
            target[0].observe(self._update_source, names=target[1])

    @contextlib.contextmanager
    def _busy_updating(self):
        self.updating = True
        try:
            yield
        finally:
            self.updating = False

    def _update_target(self, change):
        if self.updating:
            return
        with self._busy_updating():
            setattr(self.target[0], self.target[1], change.new)

    def _update_source(self, change):
        if self.updating:
            return
        with self._busy_updating():
            setattr(self.source[0], self.source[1], change.new)

    def unlink(self):
        self.source[0].unobserve(self._update_target, names=self.source[1])
        self.target[0].unobserve(self._update_source, names=self.target[1])
        self.source, self.target = None, None


# END copy


class ReadOnlyDirectionalLink:
    "Like ipywidgets.dlink, but forcibly sets the values of read-only traits on `target`"
    # Copied with modifications from https://github.com/ipython/traitlets/blob/5.0.5/traitlets/traitlets.py#L317-L364
    # NOTE: we don't just subclass `ipywidgets.dlink` because the internal implementations change a bit
    # between 4.3.3 and 5.x

    updating = False

    def __init__(self, source, target, transform=None):
        self._transform = transform if transform else lambda x: x
        _validate_link(source, target)
        self.source, self.target = source, target

        src_obj, src_trait = source
        try:
            self._update({"new": getattr(src_obj, src_trait)})
        finally:
            src_obj.observe(self._update, names=src_trait)

    @contextlib.contextmanager
    def _busy_updating(self):
        self.updating = True
        try:
            yield
        finally:
            self.updating = False

    def _update(self, change):
        if self.updating:
            return
        with self._busy_updating():
            target, target_trait = self.target
            target.set_trait(target_trait, self._transform(change["new"]))

    def unlink(self):
        self.source[0].unobserve(self._update, names=self.source[1])


class ProxytypeInstance(traitlets.Instance):
    """
    Trait type that tries to promote values to a given Proxytype

    Example
    -------
    >>> import traitlets
    >>> import descarteslabs.workflows as wf
    >>> from descarteslabs.workflows.interactive import ProxytypeInstance
    >>> class ProxyTraits(traitlets.HasTraits): # doctest: +SKIP
    ...     int_list = ProxytypeInstance(klass=wf.List[wf.Int]) # doctest: +SKIP
    ...     @traitlets.observe('int_list') # doctest: +SKIP
    ...     def int_list_changed(self, change): # doctest: +SKIP
    ...         print(f"new int list: {change['new']}") # doctest: +SKIP
    >>> pt = ProxyTraits() # doctest: +SKIP
    >>> pt.int_list = [1, 2, 3] # doctest: +SKIP
    new int list: <descarteslabs.workflows.types.containers.list_.List[Int] object at 0x...>
    >>> pt.int_list = [1, 2, "not an int"] # doctest: +SKIP
    TraitError: For parameter 'int_list', could not promote [1, 2, 'not an int']
    to <class 'descarteslabs.workflows.types.containers.list_.List[Int]'>: List[Int]:
    Expected iterable values of type <class 'descarteslabs.workflows.types.primitives.number.Int'>,
    but for item 2, got 'not an int'
    """

    def validate(self, obj, value):
        if isinstance(value, self.klass):
            return value
        else:
            try:
                return self.klass._promote(value)
            except types.ProxyTypeError as e:
                raise traitlets.TraitError(
                    "For parameter {!r}, could not promote {!r} to {}: {}".format(
                        self.name, value, self.klass, e
                    )
                )


py_type_to_trait = {
    bool: traitlets.CBool,
    int: traitlets.CInt,
    float: traitlets.CFloat,
    str: traitlets.CUnicode,
    list: traitlets.List,
    tuple: traitlets.Tuple,
    dict: traitlets.Dict,
    datetime.datetime: ipywidgets.trait_types.Datetime,
    datetime.date: ipywidgets.trait_types.Date,
}


def obj_to_trait(obj, name):
    """
    Construct a ``traitlets.TraitType`` instance suitable for holding ``obj`` as ``name``, based on its type.

    If ``type(obj)`` is in the ``py_type_to_trait`` dict, it uses the associated trait type.

    If ``obj`` is a `Proxytype`, it returns a `ProxytypeInstance` trait (which will take
    new values and attempt to promote them to that `Proxytype`.)

    Otherwise, if ``obj`` is a HasTraits instance, it will use whatever trait type that object
    uses for its ``value`` trait.

    Parameters
    ----------
    obj: bool, int, float, str, list, tuple, dict, datetime.datetime, datetime.date, Proxytype, or traitlets.HasTraits
        Create a trait to hold this value
    name: str
        Hold the trait under this name

    Returns
    -------
    trait: traitlets.TraitType
        Instantiated TraitType that could be added to a HasTraits class.

    Raises
    ------
    TypeError:
        If there's no registered TraitType to hold this type of value.

        If ``obj`` is a HasTraits instance without a ``value`` trait.
    """
    type_ = type(obj)
    try:
        return py_type_to_trait[type_](name=name)
    except KeyError:
        if isinstance(obj, types.Proxytype):
            return ProxytypeInstance(name=name, klass=type_)
        elif isinstance(obj, traitlets.HasTraits):
            try:
                # get the descriptor (trait object) for the `value` trait
                trait = type_.value
            except AttributeError:
                raise TypeError(
                    "Unsupported widget type {!r}, "
                    "since it has no `value` trait".format(type_.__name__)
                )
            else:
                new_trait = copy.deepcopy(trait)  # hope this actually works...
                new_trait.name = name
                return new_trait

    raise TypeError(
        "Cannot accept parameter of type {!r}. Must be a Proxytype, or one of: {}".format(
            type_.__name__, ", ".join(t.__name__ for t in py_type_to_trait)
        )
    )


py_type_to_widget = {
    bool: lambda **kwargs: ipywidgets.Checkbox(
        layout=ipywidgets.Layout(width="initial"), indent=False, **kwargs
    ),
    int: ipywidgets.IntText,
    float: ipywidgets.FloatText,
    str: lambda **kwargs: ipywidgets.Text(continuous_update=False, **kwargs),
    datetime.datetime: ipywidgets.DatePicker,
    datetime.date: ipywidgets.DatePicker,
}


def obj_to_widget(value, **kwargs):
    """
    Construct an ipywidget for representing ``value``.

    ``type(value)`` must be in the ``py_type_to_widget`` dict.

    Parameters
    ----------
    value: bool, int, float, str, datetime.datetime, datetime.date
        The value to make a widget control for
    **kwargs
        Additional keyword arguments to pass to the widget constructor

    Returns
    -------
    widget: ipywidgets.Widget
        Widget initialized with ``value``

    Raises
    ------
    TypeError:
        If there's no registered widget type to represent this type of value.
    """
    type_ = type(value)
    try:
        widget_type = py_type_to_widget[type_]
    except KeyError:
        raise TypeError("No widget to display a {!r}".format(type_.__name__))
    else:
        return widget_type(value=value, **kwargs)


class ParameterSet(traitlets.HasTraits):
    """
    Parameters for a `WorkflowsLayer`, which updates the layer when new values are assigned.

    A `ParameterSet` is constructed automatically when calling `.Image.visualize` and added to the `WorkflowsLayer`;
    you shouldn't construct one manually.

    You can access a widget for interactively controlling these parameters at `widget`.

    Attributes
    ----------
    widget: ipywidgets.Widget
        A widget showing a table of controls, linked to this `ParameterSet`.
        Updating the controls causes the map to update.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> imgs = wf.ImageCollection.from_id(
    ...     "sentinel-1:GRD", start_datetime=wf.parameter("start", wf.Datetime)
    ... )
    >>> filtered = imgs.filter(lambda img: img.properties['pass'] == wf.parameter("pass_dir", wf.Str))
    >>> composite = imgs.mean(axis="images").pick_bands("vv")
    >>> lyr = composite.visualize("vv mean", start=wf.Datetime(2018), pass_dir="ASCENDING")  # doctest: +SKIP
    >>> params = lyr.parameters  # doctest: +SKIP
    >>> # ^ get the ParameterSet for the layer
    >>> params.pass_dir  # doctest: +SKIP
    "ASCENDING"
    >>> params.pass_dir = "DESCENDING"  # doctest: +SKIP
    >>> # ^ this updates the layer on the map
    >>> params.start = "2019-01-01"  # doctest: +SKIP
    >>> # ^ as does this
    >>> params.link("start", my_ipywidget)  # doctest: +SKIP
    >>> # ^ this links the "start" parameter to an ipywidget's value

    Notes
    -----
    The names and types of fields on a `ParameterSet` are fixed,
    and can only be changed using `update`. This means that on
    a `ParameterSet` that only has the field ``x``, which holds a float,
    ``params.x = "foo"`` will fail (wrong type), as will ``params.y = "bar"``
    (``y`` doesn't exist).

    When `.Image.visualize` creates a `ParameterSet` for you, it adds fields for
    whichever parameter the imagery depends on. If ``img`` depends on
    ``wf.widgets.slider("slidey", 0, 1)`` and ``wf.parameter("td", wf.Timedelta)``
    for example, then ``img.visualize("my layer", td=wf.Timedelta(days=2))``
    will create the fields ``slidey`` and ``td``. More importantly, it infers the *type*
    of those fields from their parameter types, so ``slidey`` would only accept floats,
    and ``td`` would only accept Timedeltas.

    Therefore, if you experience a ``TypeError`` assiging to a `ParameterSet` field,
    you may need to change types of the initial parameters ``img`` depends on.
    """

    def __init__(self, notify_object, notify_name, **traits):
        """
        You shouldn't need to construct a ParameterSet manually, but here's how you would:

        Parameters
        ----------
        notify_object: traitlets.HasTraits instance
            The object to notify when any of the traits on this `ParameterSet` change
        notify_name: str
            The ``name`` to use in the change notification sent to that object
        **traits: traitlets.TraitType instances
            The traits to add to this `ParameterSet`
        """
        self._notify_object = notify_object
        self._notify_name = notify_name
        self._links = {}
        self.add_traits(**traits)

        self._order = tuple(traits)
        self.widget = self.make_widget()

    @traitlets.observe(traitlets.All, type=traitlets.All)
    def _on_change(self, change):
        self._notify_object.notify_change(
            traitlets.Bunch(change, name=self._notify_name, key=change["name"])
        )
        # ^ NOTE(gabe): Bunch is workaround for traitlets bug https://github.com/ipython/traitlets/pull/536

        new_contents = self._make_widget_contents()
        self.widget.children = new_contents

    def link(self, name, target, attr="value"):
        """
        Link an attribute to an ipywidget (or other object).

        If a link to the attribute was previously created, it is unlinked.

        Parameters
        ----------
        name: str
            The name of the parameter to link
        target: ipywidgets.Widget, any traitlets.HasTraits instance, or None
            The object to link to.
            If None, any existing link to ``name`` is removed.
        attr: str, default "value"
            The name of the attribute on ``target`` to link to.
            Defaults to ``"value"``, since that works for most ipywidgets.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> from ipywidgets import FloatSlider
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> masked_img = img.mask(img > wf.parameter("threshold", wf.Float)) # doctest: +SKIP
        >>> layer = masked_img.visualize("sample", colormap="plasma", threshold=0.07) # doctest: +SKIP
        >>> layer.parameters.link("threshold", my_ipywidget) # doctest: +SKIP
        >>> # ^ links the "threshold" parameter to an ipywidget's value
        >>> layer2 = masked_img.visualize("sample", colormap="plasma", threshold=0.3) # doctest: +SKIP
        >>> layer2.parameters.link("threshold", layer.parameters, attr="threshold") # doctest: +SKIP
        >>> # ^ now the `threshold` parameter is linked between `layer` and `layer2`
        >>> widget = FloatSlider(min=0, max=1) # doctest: +SKIP
        >>> layer2.parameters.link("threshold", widget) # doctest: +SKIP
        >>> # ^ now `threshold` is linked to the widget, and the link is broken to `layer`
        """
        current_link = self._links.get(name, None)

        if current_link is not None:
            current_link.unlink()

        if target is not None:
            with self.hold_trait_notifications():
                link_type = (
                    ReadOnlyDirectionalLink
                    if getattr(type(self), name).read_only
                    else SimpleLink
                )
                link = link_type((target, attr), (self, name))
                self._links[name] = link
        else:
            if current_link is not None:
                del self._links[name]

    def to_dict(self):
        """
        Key-value pairs of the parameters, in order.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> masked_img = img.mask(img > wf.parameter("threshold", wf.Float)) # doctest: +SKIP
        >>> layer = masked_img.visualize("sample", colormap="plasma", threshold=0.07) # doctest: +SKIP
        >>> layer.parameters.to_dict() # doctest: +SKIP
        {'threshold': 0.07}
        """
        # return {name: getattr(self, name) for name in self._order}

        # since `add_traits`, `remove_traits` are still in the public API,
        # and we can't atomically `update` either, we must treat `self._order`
        # as just a "guideline" and handle extra names that should or shouldn't
        # be in it.
        names = set(self.trait_names())
        dct = {}
        for name in self._order:
            try:
                dct[name] = getattr(self, name)
            except AttributeError:
                pass
            else:
                names.remove(name)
        for name in names:
            dct[name] = getattr(self, name)
        return dct

    def _make_widget_contents(self, skip=None):
        if skip is None:
            skip = ()
        items = tuple((k, v) for k, v in six.iteritems(self.to_dict()) if k not in skip)
        if not items:
            return []

        names, values = zip(*items)

        labels = []
        widgets = []
        for name, value in zip(names, values):
            link = self._links.get(name, None)
            if link is not None and link.source is not None:
                widget = link.source[0]
                if not isinstance(widget, ipywidgets.Widget):
                    raise TypeError(
                        "The parameter {!r} is already linked to the non-widget {}. "
                        "To auto-generate controls for a ParameterSet, existing links must only be to widgets. "
                        "Instead, manually construct this link (with `ipywidgets.link()`, not `self.link()`) "
                        "*after* auto-generating the controls.".format(name, widget)
                    )
                label = getattr(widget, "_label", name) or name
                # ^ HACK: this magic attribute comes from `interactive/widgets`, where we stick any `label`
                # on the widget object as `_label`. Otherwise, here in `ParameterSet`, we wouldn't know
                # the longform label for the widget. And if we had actually set the label in `.description`,
                # then we'd end up with two labels for the widget (one from ParameterSet and one generated by the
                # widget itself).
            else:
                try:
                    widget = obj_to_widget(value)
                except TypeError:
                    widget = ipywidgets.Label(
                        "No widget for {!r}".format(type(value).__name__)
                    )
                else:
                    self.link(name, widget)
                label = name

            widgets.append(widget)
            labels.append(ipywidgets.Label(label))

        labels_col = ipywidgets.VBox(labels)
        widgets_col = ipywidgets.VBox(widgets)
        control = ipywidgets.HBox([labels_col, widgets_col])

        contents = [control]

        try:
            title = self._notify_object.name
        except AttributeError:
            pass
        else:
            contents.insert(0, ipywidgets.HTML("<b>{}</b>".format(title)))

        return contents

    def make_widget(self, skip=None):
        """
        Make a widget for controlling these parameters.

        Widgets that were passed in or linked are displayed.
        For values that aren't already linked to a widget, an appropriate
        widget type is chosen if possible.

        Note that this widget can become out of date and unlinked
        once `update` is called; use the `widget` property for an
        always-up-to-date widget.

        Parameters
        ----------
        skip: list[str]
            Sequence of parameter names to *not* include in the widget

        Returns
        -------
        widget: ipywidgets.Widget or None
            A widget showing a table of controls, linked to this `ParameterSet`.
            Updating the widgets causes the map to update.
            If there are no parameters to display, returns None.

        Example
        -------
        >>> import traitlets
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> masked_img = img.mask(img > wf.parameter("threshold", wf.Float)) # doctest: +SKIP
        >>> layer = masked_img.visualize("sample", colormap="plasma", threshold=0.07, sample_param=0.0) # doctest: +SKIP
        >>> layer.parameters.make_widget(skip=["sample_param"]) # doctest: +SKIP
        >>> # ^ displays a widget for modifying a layer's parameters, optionally skipping params
        """
        return ipywidgets.VBox(self._make_widget_contents(skip=skip))

    def update(self, **new_values):
        """
        Update the `ParameterSet` with new values.

        New parameters are added as fields on the `ParameterSet`,
        with their trait type inferred from the value.

        Current parameters that are not present in ``new_values``
        are removed from the `ParameterSet`.

        Passing a value of a different type to a current parameter will change
        its trait type.

        If a value is an ipywidgets Widget, it will be linked (via its ``"value"`` attribute)
        to that parameter. If a parameter was previously linked
        to a widget, and a different widget instance (or non-widget) is passed
        for its new value, the old widget is automatically unlinked.
        If the same widget instance is passed as is already linked, no change occurs.

        The `ParameterSet` will be reordered to the order of these new values.

        Parameters
        ----------
        **new_values: JSON-serializable value, Proxytype, or ipywidgets.Widget
            Parameter names to new values. Values can be Python types,
            `Proxytype` instances, or ``ipywidgets.Widget`` instances.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> from ipywidgets import FloatSlider
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> masked_img = img.mask(img > wf.parameter("threshold", wf.Float)) # doctest: +SKIP
        >>> layer = masked_img.visualize("sample", colormap="plasma", threshold=0.07) # doctest: +SKIP
        >>> scaled_img = img * wf.parameter("scale", wf.Float) + wf.parameter("offset", wf.Float) # doctest: +SKIP
        >>> with layer.hold_trait_notifications(): # doctest: +SKIP
        ...     layer.imagery = scaled_img # doctest: +SKIP
        ...     layer.parameters.update(scale=FloatSlider(min=0, max=10, value=2), offset=2.5) # doctest: +SKIP
        >>> # ^ re-use the same layer instance for a new Image with different parameters
        """
        old_order = self._order
        self._order = tuple(new_values)

        self._update_traits_for_new_values(new_values)
        self._assign_new_values(new_values)

        if self._order != old_order:
            # in case _just_ the order changed, but no values, ensure the
            # widget is still redrawn
            self.widget.children = self._make_widget_contents()

    # def update_values(self, **new_values):
    #     """
    #     Update the current traits of the `ParameterSet` with new values.

    #     Unlike `update`, this does not add, remove, or change the type of any fields.

    #     If a value is an ipywidgets Widget, it will be linked (via its ``"value"`` attribute)
    #     to that parameter. If a parameter was previously linked
    #     to a widget, and a different widget instance (or non-widget) is passed
    #     for its new value, the old widget is automatically unlinked.
    #     If the same widget instance is passed as is already linked, no change occurs.

    #     Parameters
    #     ----------
    #     **new_values: JSON-serializable value, Proxytype, or ipywidgets.Widget
    #         Parameter names to new values. Values can be Python types,
    #         `Proxytype` instances, or ``ipywidgets.Widget`` instances.

    #         All the names must already be traits of this `ParameterSet`,
    #         and all values must be compatible with those current trait types.

    #     Raises
    #     ------
    #     ValueError:
    #         If given a kwarg name that isn't already a trait
    #     TypeError:
    #         If given a value that's incompatible with the current trait type for that name
    #     """
    #     for name in new_values:
    #         if not self.has_trait(name):
    #             raise ValueError(
    #                 f"The trait {name!r} does not exist in {self.trait_names()!r}"
    #             )
    #     self._assign_new_values(new_values)

    def _update_traits_for_new_values(self, new_values):
        """
        Add, remove, and change the type of traits to be compatible with new values.

        Parameters
        ----------
        new_values: dict[str, any]
            Parameter names to new values. Values can be Python types,
            `Proxytype` instances, or ``ipywidgets.Widget`` instances.
        """
        current_values = self.to_dict()
        current_traits = self.traits()
        current_names = six.viewkeys(current_traits)
        new_names = six.viewkeys(new_values)

        add_names = new_names - current_names
        remove_names = current_names - new_names

        new_traits = {}
        # check for same name, but trait type has changed
        for changed_name in new_names - add_names - remove_names:
            old_value = current_values[changed_name]
            new_value = new_values[changed_name]

            if new_value is old_value:
                # definitely don't need to change anything
                continue

            old_trait_type = type(current_traits[changed_name])
            new_trait = obj_to_trait(new_value, changed_name)  # todo handle error?
            if old_trait_type != type(new_trait) or isinstance(
                new_value, traitlets.HasTraits
            ):
                # by golly, the trait type has changed!
                # NOTE: we always replace when given a HasTraits instance, because though
                # the type of the trait descriptor may not have changed, all its attributes could have.
                remove_names.add(changed_name)
                new_traits[changed_name] = new_trait

        for name in remove_names:
            # unlink names we're removing
            self.link(name, None)
        self.remove_traits(*remove_names)

        new_traits.update(
            {name: obj_to_trait(new_values[name], name) for name in add_names}
        )
        self.add_traits(**new_traits)

    def _assign_new_values(self, new_values):
        """
        Assign new values to traits, directly or by linking

        If given an ipywidget as a value, it's linked to that trait,
        and any previous link is unlinked

        Parameters
        ----------
        new_values: dict[str, any]
            Parameter names to new values. Values can be Python types,
            `Proxytype` instances, or ``ipywidgets.Widget`` instances.
        """
        with self.hold_trait_notifications():
            for name, value in six.iteritems(new_values):
                current_link = self._links.get(name, None)
                if current_link is not None:
                    if value is not current_link.source[0]:
                        # a new widget (or non-widget) is being used for this name; unlink the old one
                        current_link.unlink()
                        del self._links[name]
                        current_link = None  # we'll check this below

                if isinstance(value, traitlets.HasTraits):
                    if current_link is None:
                        link_type = (
                            ReadOnlyDirectionalLink
                            if getattr(type(self), name).read_only
                            else SimpleLink
                        )
                        self._links[name] = link_type((value, "value"), (self, name))
                        self._notify_trait(name, getattr(self, name), value.value)
                else:
                    self.set_trait(name, value)

    def add_traits(self, **traits):
        """
        Dynamically add trait attributes to the HasTraits instance.

        If you are manipulating a ParameterSet generated from a layer, instead
        use :meth:`update <descarteslabs.workflows.interactive.ParameterSet.update>`,
        which handles adding and removing traits in a more delclarative way.

        Example
        -------
        >>> import traitlets
        >>> import descarteslabs.workflows as wf
        >>> class Listener(traitlets.HasTraits):
        ...     @traitlets.observe("param")
        ...     def handler(self, change):
        ...         print(change['key'])
        ...         print(change)
        >>> listener = Listener()
        >>> ps = wf.interactive.ParameterSet(listener, "param", foo=traitlets.Float()) # doctest: +SKIP
        >>> ps.foo = 1.1 # doctest: +SKIP
        foo
        {'name': 'param',
         'old': 0.0,
         'new': 1.1,
         'owner': ParameterSet({'foo': 1.1}),
         'type': 'change',
         'key': 'foo'
         }
        >>> ps.bar = "baz" # doctest: +SKIP
        >>> # ^ nothing is printed, `bar` is not a trait
        >>> ps.add_traits(bar=traitlets.Unicode()) # doctest: +SKIP
        >>> ps.bar # doctest: +SKIP
        ''
        >>> ps.bar = "quix" # doctest: +SKIP
        bar
        {'name': 'param',
         'old': '',
         'new': 'quix',
         'owner': ParameterSet({'bar': 'quix', 'foo': 1.1}),
         'type': 'change',
         'key': 'bar'
         }
        """
        # Normally, `HasTraits.add_traits` dynamically constructs a new type
        # that's a subclass of `type(self)`, appends *just* the new traits to that class's `__dict__`,
        # and modifies `self.__class__` to point at the new subtype, using inheritance
        # to make access on the existing traits fall back on the parent class.
        # This makes removing traits extremely difficult, since the trait you want to remove
        # may be defined some arbitrary depth up the MRO---and there's no way to know if it's safe
        # to mutate that parent class, or if someone else is inheriting from it too.

        # Since we'd like to be able to remove traits from a `ParameterSet`, we override `add_traits`
        # to add the guarantee that instead of making a chain of subclasses for each new trait,
        # *any `ParameterSet` instance with >0 traits is a direct child class of `ParameterSet`,
        # and all `TraitType`s are set on that class's `__dict__`*.

        # Achieving this is pretty simple: rather than having our new type inherit from our current type
        # (causing chaining), we just always inherit from the base `ParameterSet`. And rather than just
        # adding the new `traits` to that new type's `__dict__`, we also copy in all the traits defined
        # on our current type's `__dict__`, so everything is in one place, and easy to remove.

        # Copied and modified from:
        # https://github.com/ipython/traitlets/blob/41551bc8b30ccc28af738e93615e3408cb94d5d3/traitlets/traitlets.py#L1405-L1415

        # NOTE(gabe): there's a simpler(-ish) way we could do this, by just mutating `type(self)` directly
        # and `setattr`ing the new traits in, but I was worried that if the base implementation of `add_traits`
        # changed, that could break. Either way, this is rather brittle to traitlets changes, but fully overriding
        # the method seems less prone to surprises (so long as the `MetaHasDescriptors` metaclass doesn't change much).

        cls = self.__class__
        attrs = {"__module__": cls.__module__}
        if hasattr(cls, "__qualname__"):
            # __qualname__ introduced in Python 3.3 (see PEP 3155)
            attrs["__qualname__"] = cls.__qualname__
        attrs.update(self.traits())
        # ^ CHANGED: add in current traits to new type's `__dict__`
        attrs.update(traits)
        self.__class__ = type(cls.__name__, (ParameterSet,), attrs)
        # ^ CHANGED: always use ParameterSet as base class to prevent guarantee 1-depth inheritance
        for trait in traits.values():
            trait.instance_init(self)

    def remove_traits(self, *names):
        """
        Remove traits that were dynamically added to the HasTraits instance

        If you are manipulating a ParameterSet generated from a layer, instead
        use :meth:`update <descarteslabs.workflows.interactive.ParameterSet.update>`,
        which handles adding and removing traits in a more delclarative way.

        Example
        -------
        >>> import traitlets
        >>> import descarteslabs.workflows as wf
        >>> class Listener(traitlets.HasTraits):
        ...     @traitlets.observe("param")
        ...     def handler(self, change):
        ...         print(change['key'])
        ...         print(change)
        >>> listener = Listener()
        >>> ps = wf.interactive.ParameterSet(listener, "param", foo=traitlets.Float()) # doctest: +SKIP
        >>> ps.foo = 1.1 # doctest: +SKIP
        foo
        {'name': 'param',
         'old': 0.0,
         'new': 1.1,
         'owner': ParameterSet({'foo': 1.1}),
         'type': 'change',
         'key': 'foo'
         }
        >>> ps.add_traits(bar=traitlets.Unicode()) # doctest: +SKIP
        >>> ps.bar = 'quix' # doctest: +SKIP
        bar
        {'name': 'param',
         'old': '',
         'new': 'quix',
         'owner': ParameterSet({'bar': 'quix', 'foo': 1.1}),
         'type': 'change',
         'key': 'bar'
         }
        >>> ps.remove_traits("foo") # doctest: +SKIP
        >>> ps.foo = 2.2 # doctest: +SKIP
        >>> # ^ nothing is printed, `foo` is no longer a trait
        >>> ps.to_dict() # doctest: +SKIP
        {'bar': 'quix'}
        """
        # Thanks to our guarantee from our `add_traits` that:
        # - `type(self)` is a singleton class and nobody else cares about it
        # - all relevant `TraitType`s are set on `type(self).__dict__` and nowhere else
        # to remove traits... we just delete them from `type(self)`. Pretty simple.
        if not names:
            return

        cls = type(self)
        for name in names:
            try:
                old = self._trait_values.pop(name)
            except KeyError:
                raise ValueError("The trait {} does not exist on {}".format(name, self))
            delattr(cls, name)
            # to play by the rules (and make the map update on parameter deletion),
            # we notify that the trait is deleted. but `delete` is a non-standard
            # event, so don't expect anyone to be listening.
            self.notify_change(
                traitlets.Bunch(
                    name=name,
                    old=old,
                    new=traitlets.Undefined,
                    owner=self,
                    type="delete",
                )
            )

    def __repr__(self):
        return "{}({})".format(type(self).__name__, self.to_dict())
