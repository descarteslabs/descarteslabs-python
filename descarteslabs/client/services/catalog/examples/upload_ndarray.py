from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.catalog import Catalog
from descarteslabs.client.services.metadata import Metadata

raster = Raster()
catalog = Catalog()
metadata = Metadata()

id_ = "5151d2825f5e29ff129f86d834946363ff3f7e57:modis:09:CREFL_v2_test:2017-01-01-1835_11N_07_MO_09_v2"
scene_meta = metadata.get(id_)
r, meta = raster.ndarray(id_, bands=["red"])

p = catalog.add_product(
    "test_nda_upload",
    description="Test uploading georeferenced ndarrays",
    title="Test ndarray upload",
)

band_spec = meta["bands"][0]["description"]
catalog.add_band(
    p["data"]["id"],
    "red",
    srcband=1,
    jpx_layer=0,
    **{
        k: v
        for k, v in band_spec.items()
        if k not in ["product", "id", "name", "read", "owner", "owner_type"]
    }
)

catalog.upload_ndarray(
    r, p["data"]["id"], id_[41:], raster_meta=meta, acquired=scene_meta["acquired"]
)
