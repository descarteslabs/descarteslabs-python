import json

from ...catalog import Image, DerivedBand
from ...common.dotdict import DotDict
from ...catalog.tests import mock_data as catalog_mock_data

V1_COMPATIBILITY = "v1_compatibility"


def _image_get(image_id, request_params=None):
    image = catalog_mock_data._image_get(image_id)
    if request_params and request_params.get(V1_COMPATIBILITY):
        # doctor up some v1 properties
        new_image = Image(id=image.id, _saved=True)
        new_image._attributes = dict(**image._attributes)
        new_image._attributes["v1_properties"] = {
            "id": image.id,
            "product": image.product_id,
            "cs_code": image.cs_code,
            "proj4": image.projection,
        }
        image = new_image
    return image


def _cached_bands_by_product(product_id, _client):
    bands = catalog_mock_data._cached_bands_by_product(product_id, _client)
    return DotDict(
        {
            n: {
                "id": b.id,
                "name": b.name,
                "function_name": b.function_name,
                "bands": b.bands,
                "data_type": b.data_type,
                "data_range": b.data_range,
            }
            if isinstance(b, DerivedBand)
            else {
                "id": b.id,
                "name": b.name,
                "product": b.product_id,
                "data_type": b.data_type,
                "data_range": b.data_range,
            }
            for n, b in bands.items()
        }
    )


SEARCH = {
    r'{"filter": "[{\"op\":\"eq\",\"name\":\"product_id\",\"val\":\"landsat:LC08:PRE:TOAR\"}]", "include": "product", "intersects": "{\"type\":\"Polygon\",\"coordinates\":[[[-95.836498,39.278486],[-92.068696,39.278486],[-92.068696,42.799988],[-95.836498,42.799988],[-95.836498,39.278486]]]}", "limit": 4}': [  # noqa: E501
        catalog_mock_data.IMAGES.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"),
        catalog_mock_data.IMAGES.get("landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"),
    ],
    r'{"filter": "[{\"op\":\"eq\",\"name\":\"product_id\",\"val\":\"landsat:LC08:PRE:TOAR\"},{\"and\":[{\"name\":\"acquired\",\"op\":\"gte\",\"val\":\"2016-07-06\"},{\"name\":\"acquired\",\"op\":\"lt\",\"val\":\"2016-07-16\"}]},{\"name\":\"cloud_fraction\",\"op\":\"lt\",\"val\":0.6},{\"op\":\"eq\",\"name\":\"storage_state\",\"val\":\"available\"},{\"name\":\"bright_fraction\",\"op\":\"lt\",\"val\":0.3}]", "include": "product", "intersects": "{\"type\":\"Polygon\",\"coordinates\":[[[-95.836498,39.278486],[-92.068696,39.278486],[-92.068696,42.799988],[-95.836498,42.799988],[-95.836498,39.278486]]]}", "limit": 4, "sort": "-acquired"}': [  # noqa: E501
        catalog_mock_data.IMAGES.get("landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"),
        catalog_mock_data.IMAGES.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"),
    ],
}


def _image_search(self):
    url_next, params = self._to_request()
    if url_next is not None:
        v1_compatibility = params.pop(V1_COMPATIBILITY, None)
        params = json.dumps(params, sort_keys=True)
        images = SEARCH.get(params, [])
        for image in images:
            if v1_compatibility:
                # doctor up some v1 properties
                new_image = Image(id=image.id, _saved=True)
                new_image._attributes = dict(**image._attributes)
                new_image._attributes["v1_properties"] = {
                    "id": image.id,
                    "product": image.product_id,
                    "cs_code": image.cs_code,
                    "proj4": image.projection,
                }
                image = new_image
            yield image


# carry this one over for convenience
_raster_ndarray = catalog_mock_data._raster_ndarray
