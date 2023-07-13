import cachetools

from ..catalog import Band, DerivedBand, Search
from ..common.dotdict import DotDict
from ..common.property_filtering import Properties

REQUEST_PARAMS = {"v1_compatibility": True}

BANDS_BY_PRODUCT_CACHE = cachetools.TTLCache(maxsize=256, ttl=600)


# this is just like the one in catalog, except it uses v1 properties and DotDict
@cachetools.cached(BANDS_BY_PRODUCT_CACHE, key=lambda p, c: p)
def cached_bands_by_product(product_id, client):
    bands = DotDict(
        {
            band.name: DotDict(band.v1_properties)
            for band in Band.search(
                client=client, request_params=REQUEST_PARAMS
            ).filter(Properties().product_id == product_id)
        }
    )
    bands.update(
        {
            band.id: DotDict(band.v1_properties)
            for band in Search(
                DerivedBand,
                url=f"/products/{product_id}/relationships/derived_bands",
                client=client,
                includes=False,
                request_params=REQUEST_PARAMS,
            )
        }
    )
    return bands
