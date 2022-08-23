import cachetools


@cachetools.cached(cachetools.TTLCache(maxsize=256, ttl=600), key=lambda p, c: p)
def cached_bands_by_product(product_id, metadata_client):
    return metadata_client.get_bands_by_product(product_id)
