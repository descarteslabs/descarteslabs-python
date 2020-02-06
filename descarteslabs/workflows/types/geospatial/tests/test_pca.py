# from ...containers import List
# from ...primitives import Float
# from .. import PCA, Image, ImageCollection
#
#
# def test_all_methods():
#    img = Image.from_id("foo")
#    col = ImageCollection.from_id("bar")
#    pca = PCA("constant", 0.1, 3)
#
#    assert isinstance(pca, PCA)
#    assert isinstance(pca.fit(img), PCA)
#    assert isinstance(pca.fit(col), PCA)
#    assert isinstance(pca.fit_transform(img), Image)
#    assert isinstance(pca.fit_transform(col), ImageCollection)
#    assert isinstance(pca.transform(img), Image)
#    assert isinstance(pca.transform(col), ImageCollection)
#    assert isinstance(pca.score(img), Float)
#    assert isinstance(pca.components(), List)
#    assert isinstance(pca.explained_variance(), List)
#    assert isinstance(pca.get_covariance(), List)
