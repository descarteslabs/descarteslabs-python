from descarteslabs.common.graft import client


from ..core import typecheck_promote, GenericProxytype
from ..containers import Dict, Tuple
from ..function import Function

from .image import Image
from .imagecollection import ImageCollection


class ImageCollectionGroupby(GenericProxytype):
    """
    Dict-like object for a grouped `ImageCollection`.
    """

    @typecheck_promote(ImageCollection, lambda self: Function[Image, {}, self.key_type])
    def __init__(self, imgs, func):
        """
        You should construct `ImageCollectionGroupby` from `.ImageCollection.groupby` in most cases.

        Parameters
        ----------
        imgs: ImageCollection
            `ImageCollection` to group
        func: Function
            Key function which takes an `Image` and returns which group the `Image` belongs to.
            Must return an instance of ``self.key_type``.

        Returns
        -------
        grouped: ImageCollectionGroupby
        """
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic {}; the item type must be specified".format(
                    type(self).__name__
                )
            )

        self.graft = client.apply_graft("ImageCollectionGroupby.create", imgs, func)

        # store this once so that repeated calls to `.groups` refer to the same object in the graft
        self._groups = Dict[self.key_type, ImageCollection]._from_apply(
            "ImageCollectionGroupby.groups", self
        )

    @property
    def key_type(self):
        "Proxytype: The type of the group keys"
        return self._type_params[0]

    @property
    def groups(self):
        "`Dict` of group key -> `ImageCollection`"
        return self._groups

    def one(self):
        "A `Tuple` of (group key, `ImageCollection`) for one random group. Helpful for debugging."
        return Tuple[self.key_type, ImageCollection]._from_apply(
            "ImageCollectionGroupby.one", self
        )

    @typecheck_promote(lambda self: self.key_type)
    def __getitem__(self, idx):
        return ImageCollection._from_apply("ImageCollectionGroupby.get", self, idx)

    def compute(self, *args, **kwargs):
        raise TypeError(
            "{} cannot be computed directly. "
            "Instead, compute `.groups`, use `.map` or `.mean`, etc. to composite the groups "
            "into a single Image/ImageCollection, or pick one group with [] syntax "
            "and compute that.".format(type(self).__name__)
        )

    def map(self, func):
        """
        Apply a function to each group as an `ImageCollection` and combine the results.

        ``func`` must take two arguments: the group, and an `ImageCollection` of all Images in that group.

        If it returns an `ImageCollection`, the ImageCollections for all groups will be concatenated together.

        If it returns an `Image`, those Images will be combined into a single ImageCollection.

        If it returns any other type, `map` will return a `Dict`, where keys are groups
        and values are results of ``func``.

        Note that every `Image` in every `ImageCollecion` gets a ``"group"`` field added to its `~.Image.properties`,
        so that field will be merged/dropped according to normal metadata broadcasting rules (i.e. will still be present
        on an `Image` composited from a group's `ImageCollection`, since it's the same for every `Image`.)
        """
        proxy_func = Function.from_callable(func, self.key_type, ImageCollection)
        result_type = proxy_func._type_params[-1]

        if result_type in (ImageCollection, Image):
            out_type = ImageCollection
            func = "ImageCollectionGroupby.map_ic"
        else:
            out_type = Dict[self.key_type, result_type]
            func = "ImageCollectionGroupby.map"
        return out_type._from_apply(func, self, proxy_func)

    def count(self, axis=None):
        "Apply `.ImageCollection.count` to each group"
        return self.map(lambda group, imgs: imgs.count(axis=axis))

    def sum(self, axis=None):
        "Apply `.ImageCollection.sum` to each group"
        return self.map(lambda group, imgs: imgs.sum(axis=axis))

    def min(self, axis=None):
        "Apply `.ImageCollection.min` to each group"
        return self.map(lambda group, imgs: imgs.min(axis=axis))

    def max(self, axis=None):
        "Apply `.ImageCollection.max` to each group"
        return self.map(lambda group, imgs: imgs.max(axis=axis))

    def mean(self, axis=None):
        "Apply `.ImageCollection.mean` to each group"
        return self.map(lambda group, imgs: imgs.mean(axis=axis))

    def median(self, axis=None):
        "Apply `.ImageCollection.median` to each group"
        return self.map(lambda group, imgs: imgs.median(axis=axis))

    def std(self, axis=None):
        "Apply `.ImageCollection.std` to each group"
        return self.map(lambda group, imgs: imgs.std(axis=axis))
