# from ...cereal import serializable
# from ..core import typecheck_promote
# from ..containers import Struct, List
# from ..primitives import Int, Float, Str
# from .image import Image
# from .imagecollection import ImageCollection
#
#
# PCABase = Struct[{"strategy": Str, "fill_value": Float, "n_components": Int}]
#
#
# @serializable(is_named_concrete_type=True)
# class PCA(PCABase):
#    """
#    Object for applying Principal Component Analysis on an `Image` or `ImageCollection`
#    along the ``bands`` dimension.
#    Masked values are filled in using ``strategy``: either with the mean for that band or ``fill_value``.
#
#    If using `.fit` and `.transform` instead of `.fit_transform` the number of bands
#    must be the same for the object called with `.fit` and the object called with `.transform`.
#
#    Band names on the transformed `Image` or `ImageCollection` will be of the format 'principle_component_<num>'
#    with 'principle_component_1' corresponding to the first principle component and so on.
#
#    Examples
#    --------
#    >>> import descarteslabs.workflows as wf
#    >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#    >>> pca
#    <descarteslabs.workflows.types.geospatial.pca.PCA object at 0x...>
#    >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#    >>> fit_pca = pca.fit(img)
#    >>> img_pca = pca.transform(img)
#    >>> img_pca.compute(geoctx) # doctest: +SKIP
#    ImageResult:
#      * ndarray: MaskedArray<shape=(4, 512, 512), dtype=float64>
#      * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
#      * bandinfo: 'principle_component_1', 'principle_component_2', ...
#      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
#    """
#
#    _constructor = "wf.PCA.create"
#    _optional = {"strategy", "fill_value", "n_components"}
#
#    _doc = {
#        "strategy": """\
#            One of mean or constant.
#            The strategy used for imputing missing values.
#            """,
#        "fill_value": """\
#            When ``strategy=constant``, this is used to replace all occurences of missing values.
#            """,
#        "n_components": """\
#            Number of components to keep. If n_components is not set, all components are kept.
#            """,
#    }
#
#    def __init__(self, strategy="mean", fill_value=None, n_components=None):
#        return super(PCA, self).__init__(
#            strategy=strategy, fill_value=fill_value, n_components=n_components
#        )
#
#    @typecheck_promote((Image, ImageCollection))
#    def fit(self, obj):
#        """Fit the model.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> pca.fit(img)
#        <descarteslabs.workflows.types.geospatial.pca.PCA object at 0x...>
#        """
#        return self._from_apply("wf.PCA.fit", self, obj)
#
#    @typecheck_promote((Image, ImageCollection))
#    def fit_transform(self, obj):
#        """Fit the model and apply dimensionality reduction.
#
#        If ``obj`` is an empty Image/ImageCollection, returns the empty.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> pca.fit_transform(img).compute(geoctx) # doctest: +SKIP
#        ImageResult:
#            * ndarray: MaskedArray<shape=(4, 512, 512), dtype=float64>
#            * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
#            * bandinfo: 'principle_component_1', 'principle_component_2', ...
#            * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
#        """
#        return type(obj)._from_apply("wf.PCA.fit_transform", self, obj)
#
#    @typecheck_promote((Image, ImageCollection))
#    def transform(self, obj):
#        """Apply dimensionality reduction.
#
#        If ``obj`` is an empty Image/ImageCollection, returns the empty.
#        If the model is fit on an empty Image/ImageCollection, will always return an empty.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> fit_pca = pca.fit(img)
#        >>> pca.transform(img).compute(geoctx) # doctest: +SKIP
#        ImageResult:
#            * ndarray: MaskedArray<shape=(4, 512, 512), dtype=float64>
#            * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
#            * bandinfo: 'principle_component_1', 'principle_component_2', ...
#            * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
#        """
#        return type(obj)._from_apply("wf.PCA.transform", self, obj)
#
#    @typecheck_promote((Image, ImageCollection))
#    def score(self, obj):
#        """The average log-likelihood of all samples.
#
#        If ``obj`` is an empty Image/ImageCollection, returns None.
#        If the model is fit on an empty Image/ImageCollection, returns None.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> fit_pca = pca.fit(img)
#        >>> fit_pca.score(img).compute(geoctx) # doctest: +SKIP
#        150.70311325376323
#        """
#        return Float._from_apply("wf.PCA.score", self, obj)
#
#    def components(self):
#        """Principal axes in feature space, representing the directions of maximum variance in the data.
#        The components are sorted by explained_variance. The array shape will be (n_components x n_bands)
#        where 'n_bands' is the number of bands on the object used to fit the model.
#
#        If the model is fit on an empty Image/ImageCollection, returns an empty list.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> fit_pca = pca.fit(img)
#        >>> fit_pca.components().compute(geoctx) # doctest: +SKIP
#        [[-0.19245005302246807,
#          -0.19245005302246823,
#        ...
#        """
#        return List[List[Float]]._from_apply("wf.PCA.components", self)
#
#    def explained_variance(self):
#        """The amount of variance explained by each of the selected components.
#
#        If the model is fit on an empty Image/ImageCollection, returns an empty list.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> fit_pca = pca.fit(img)
#        >>> fit_pca.explained_variance().compute(geoctx) # doctest: +SKIP
#        [5.185159471263591,
#         3.594187229623078e-05,
#         9.551961156579615e-06,
#         3.7431008990160355e-34]
#        """
#        return List[Float]._from_apply("wf.PCA.explained_variance", self)
#
#    def get_covariance(self):
#        """Compute data covariance with the generative model.
#
#        If the model is fit on an empty Image/ImageCollection, returns an empty list.
#
#        Example
#        -------
#        >>> import descarteslabs.workflows as wf
#        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
#        >>> pca = wf.PCA(strategy="constant", fill_value=0.1, n_components=4)
#        >>> fit_pca = pca.fit(img)
#        >>> fit_pca.get_covariance().compute(geoctx) # doctest: +SKIP
#        [[0.192044152153863,
#          0.19204415215386159,
#        ...
#        """
#        return List[List[Float]]._from_apply("wf.PCA.get_covariance", self)
#
#    def compute(self, *args, **kwargs):
#        "PCA objects cannot be directly computed. Instead, compute `.transform`, `.components` etc."
#        raise TypeError(
#            "{} cannot be computed directly. "
#            "Instead, compute `.fit_transform`, `.transform`, `.components` etc.".format(
#                type(self).__name__
#            )
#        )
