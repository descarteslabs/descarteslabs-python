# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime
import json
import numpy as np
import shapely.geometry

from ...catalog import *

# flake8: noqa: E501

# this file contains mock data for the Metadata and Raster services, extracted from the production
# services, which is shared across multiple tests in this directory.

IMAGES = {
    "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1": Image(
        acquired=datetime.fromisoformat("2016-07-06T16:59:42.753476+00:00"),
        cs_code="EPSG:32615",
        product_id="landsat:LC08:PRE:TOAR",
        bits_per_pixel=[0.836, 1.767, 0.804],
        id="landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
        cloud_fraction=0.5646,
        solar_azimuth_angle=131.36710631,
        bright_fraction=0.2848,
        files=[
            File(
                hash="5b12fa74275aee3234428fc996429256",
                href="gs://descartes-l8/2016-07-06_027031_L8_432.jp2",
                size_bytes=49721086,
            ),
            File(
                hash="efb979aeda1b2fbd58fd689f84540165",
                href="gs://descartes-l8/2016-07-06_027031_L8_567_19a.jp2",
                size_bytes=43577223,
            ),
        ],
        area=35619.4,
        alt_cloud_fraction=0.3264,
        fill_fraction=0.6319,
        x_pixels=15696,
        y_pixels=15960,
        reflectance_scale=[
            0.1781,
            0.1746,
            0.1907,
            0.2252,
            0.3711,
            1.4732,
            4.5285,
            0.903,
            0.1999,
        ],
        solar_elevation_angle=64.12277058,
        confidence_dlsr=1.0,
        name="meta_LC80270312016188_v1",
        roll_angle=-0.001,
        provider_id="LC80270312016188LGN00.tar.bz",
        geometry=shapely.geometry.shape(
            {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-95.2989209, 42.7999878],
                        [-93.1167728, 42.3858464],
                        [-93.7138666, 40.703737],
                        [-95.8364984, 41.1150618],
                        [-95.2989209, 42.7999878],
                    ]
                ],
            }
        ),
        created=datetime.utcfromtimestamp(1468251918),
        published=datetime.fromisoformat("2016-07-06T23:11:30+00:00"),
        satellite_id="LANDSAT_8",
        geotrans=[258292.5, 15.0, 0.0, 4743307.5, 0.0, -15.0],
        _saved=True,
    ),
    "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1": Image(
        acquired=datetime.fromisoformat("2016-07-15T16:53:59.495435+00:00"),
        cs_code="EPSG:32615",
        product_id="landsat:LC08:PRE:TOAR",
        bits_per_pixel=[1.022, 2.61, 0.804],
        id="landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        cloud_fraction=0.1705,
        solar_azimuth_angle=129.79642888,
        bright_fraction=0.0571,
        files=[
            File(
                hash="c80038509ca5572ecdba473bc3931fab",
                href="gs://descartes-l8/2016-07-15_026032_L8_432.jp2",
                size_bytes=60751671,
            ),
            File(
                hash="92b10252278663b0b2d438bfa6c6b494",
                href="gs://descartes-l8/2016-07-15_026032_L8_567_19a.jp2",
                size_bytes=56534564,
            ),
        ],
        area=35599.3,
        alt_cloud_fraction=0.0947,
        fill_fraction=0.6439,
        x_pixels=15536,
        y_pixels=15816,
        reflectance_scale=[
            0.1786,
            0.1751,
            0.1913,
            0.2258,
            0.3721,
            1.4773,
            4.5414,
            0.9055,
            0.2005,
        ],
        solar_elevation_angle=63.72682179,
        confidence_dlsr=1.0,
        name="meta_LC80260322016197_v1",
        roll_angle=-0.001,
        provider_id="LC80260322016197LGN00.tar.bz",
        geometry=shapely.geometry.shape(
            {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-94.2036617, 41.3717716],
                        [-92.0686956, 40.9629603],
                        [-92.6448116, 39.2784859],
                        [-94.724166, 39.6850062],
                        [-94.2036617, 41.3717716],
                    ]
                ],
            }
        ),
        created=datetime.utcfromtimestamp(1469372319),
        published=datetime.fromisoformat("2016-07-22T04:49:40+00:00"),
        satellite_id="LANDSAT_8",
        geotrans=[348592.5, 15, 0, 4582807.5, 0, -15],
        _saved=True,
    ),
    "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1": Image(
        acquired=datetime.fromisoformat("2017-11-01T00:00:00+00:00"),
        area=1236433958410.1,
        bits_per_pixel=[16.24028611111111, 2.332976111111111],
        files=[
            File(
                hash="dcab9cdeace57fa4a275e51892e1d95f",
                href="gs://dl-satin_modis-mod11a2-006_r/modis:mod11a2:006/MOD11A2.A2017305.h09v05.006.2017314042814.UInt16.tif",
                size_bytes=5846503,
            ),
            File(
                hash="6c026381b1e03c6b2ddbdda96a4358c0",
                href="gs://dl-satin_modis-mod11a2-006_r/modis:mod11a2:006/MOD11A2.A2017305.h09v05.006.2017314042814.Byte.tif",
                size_bytes=4199357,
            ),
        ],
        fill_fraction=0.9842958333333334,
        geometry=shapely.geometry.shape(
            {
                "coordinates": [
                    [
                        [-117.48665603990703, 39.999999999999154],
                        [-104.4325831465788, 39.999999999999154],
                        [-102.94076527145069, 38.99999999999963],
                        [-102.22229259882958, 38.49999999999986],
                        [-100.83779312081948, 37.500000000000334],
                        [-99.52020554825341, 36.5000000000008],
                        [-97.66196710091624, 35.00000000000151],
                        [-96.49743588031265, 34.00000000000198],
                        [-94.85512379473369, 32.500000000002686],
                        [-93.82621572912193, 31.50000000000316],
                        [-92.37604307034178, 30.000000000003865],
                        [-103.92304845413969, 30.000000000003865],
                        [-105.55449269526743, 31.50000000000316],
                        [-106.71201426908073, 32.500000000002686],
                        [-108.55961536535716, 34.00000000000198],
                        [-109.86971298853625, 35.00000000000151],
                        [-111.24611797498575, 36.00000000000104],
                        [-111.96023124179068, 36.5000000000008],
                        [-113.4425172609276, 37.500000000000334],
                        [-115.00007917368902, 38.49999999999986],
                        [-115.8083609303878, 38.99999999999963],
                        [-117.48665603990703, 39.999999999999154],
                    ]
                ],
                "type": "Polygon",
            }
        ),
        geotrans=[
            -10007554.677899,
            926.6254331391661,
            0.0,
            4447802.079066,
            0.0,
            -926.6254331383334,
        ],
        id="modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1",
        provider_id="MOD11A2.A2017305.h09v05.006.2017314042814",
        name="meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1",
        modified=datetime.fromisoformat("2018-11-12T03:25:33.871326+00:00"),
        created=datetime.fromisoformat("2018-11-12T03:25:31+00:00"),
        product_id="modis:mod11a2:006",
        projection="+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs ",
        published=datetime.fromisoformat("2017-11-10T00:00:00+00:00"),
        x_pixels=1200,
        y_pixels=1200,
        satellite_id="modis",
        _saved=True,
    ),
    "modis:mod11a2:006:meta_MOD11A2.A2000049.h08v05.006.2015058135046_v1": Image(
        acquired=datetime.fromisoformat("2000-02-18T00:00:00+00:00"),
        area=1236433958407.88,
        bits_per_pixel=[5.416830555555555, 0.8889755555555555],
        files=[
            File(
                hash="19ab952b0ee2997a26cc854b0b1225ba",
                href="gs://dl-satin_modis-mod11a2-006_r/modis:mod11a2:006/MOD11A2.A2000049.h08v05.006.2015058135046.UInt16.tif",
                size_bytes=1950059,
            ),
            File(
                hash="7cdcc274be7bd6b0a206949cc8978ced",
                href="gs://dl-satin_modis-mod11a2-006_r/modis:mod11a2:006/MOD11A2.A2000049.h08v05.006.2015058135046.Byte.tif",
                size_bytes=1600156,
            ),
        ],
        fill_fraction=0.5317729166666667,
        geometry=shapely.geometry.shape(
            {
                "coordinates": [
                    [
                        [-130.5407289332235, 39.999999999999154],
                        [-117.48665603990703, 39.999999999999154],
                        [-115.8083609303878, 38.99999999999963],
                        [-115.00007917368902, 38.49999999999986],
                        [-113.4425172609276, 37.500000000000334],
                        [-111.96023124179068, 36.5000000000008],
                        [-111.24611797498575, 36.00000000000104],
                        [-109.86971298853625, 35.00000000000151],
                        [-108.55961536535716, 34.00000000000198],
                        [-106.71201426908073, 32.500000000002686],
                        [-105.55449269526743, 31.50000000000316],
                        [-103.92304845413969, 30.000000000003865],
                        [-115.47005383792722, 30.000000000003865],
                        [-117.28276966140238, 31.50000000000316],
                        [-118.56890474341712, 32.500000000002686],
                        [-119.92049433351035, 33.50000000000222],
                        [-120.6217948503908, 34.00000000000198],
                        [-122.0774588761453, 35.00000000000151],
                        [-123.606797749978, 36.00000000000104],
                        [-124.40025693531675, 36.5000000000008],
                        [-126.04724140102435, 37.500000000000334],
                        [-127.77786574853697, 38.49999999999986],
                        [-128.67595658931336, 38.99999999999963],
                        [-130.5407289332235, 39.999999999999154],
                    ]
                ],
                "type": "Polygon",
            }
        ),
        geotrans=[
            -11119505.197665,
            926.6254331383342,
            0.0,
            4447802.079066,
            0.0,
            -926.6254331383334,
        ],
        id="modis:mod11a2:006:meta_MOD11A2.A2000049.h08v05.006.2015058135046_v1",
        provider_id="MOD11A2.A2000049.h08v05.006.2015058135046",
        name="meta_MOD11A2.A2000049.h08v05.006.2015058135046_v1",
        modified=datetime.fromisoformat("2018-11-15T21:12:31.874628+00:00"),
        created=datetime.fromisoformat("2018-11-15T21:12:30+00:00"),
        product_id="modis:mod11a2:006",
        projection="+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs ",
        published=datetime.fromisoformat("2015-02-27T00:00:00+00:00"),
        x_pixels=1200,
        y_pixels=1200,
        satellite_id="modis",
        storage_state=StorageState.AVAILABLE,
        _saved=True,
    ),
}


def _image_get(id):
    return IMAGES[id]


BANDS_BY_PRODUCT = {
    "landsat:LC08:PRE:TOAR": {
        "qa_cloud": ClassBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Cloud Classification",
            tags=["class", "cloud", "30m", "landsat"],
            data_type="UInt16",
            vendor_band_name="qa_cloud",
            band_index=3,
            id="landsat:LC08:PRE:TOAR:qa_cloud",
            name="qa_cloud",
            file_index=1,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            data_range=[0, 3],
            jpx_layer_index=1,
            display_range=[0, 3],
            _saved=True,
        ),
        "tirs1": SpectralBand(
            description="Thermal infrared TIRS 1",
            tags=["spectral", "thermal", "tirs1", "100m", "landsat"],
            data_type="UInt16",
            jpx_layer_index=3,
            vendor_band_name_vendor="B10",
            product_id="landsat:LC08:PRE:TOAR",
            sort_order=10,
            vendor_order=10,
            physical_range=[-32, 64],
            band_index=2,
            id="landsat:LC08:PRE:TOAR:tirs1",
            name="tirs1",
            file_index=1,
            resolution=Resolution(value=100, unit=ResolutionUnit.METERS),
            data_range=[0, 16383],
            wavelength_nm_fwhm=600,
            wavelength_nm_min=10600,
            wavelength_nm_max=11200,
            display_range=[0, 16383],
            _saved=True,
        ),
        "qa_water": ClassBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Water Classification",
            tags=["class", "water", "30m", "landsat"],
            data_type="UInt16",
            vendor_band_name="qa_water",
            band_index=0,
            id="landsat:LC08:PRE:TOAR:qa_water",
            name="qa_water",
            file_index=1,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            data_range=[0, 3],
            jpx_layer_index=1,
            display_range=[0, 3],
            _saved=True,
        ),
        "alpha": MaskBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Alpha (valid data)",
            tags=["mask", "alpha", "15m", "landsat"],
            data_type="UInt16",
            resolution=Resolution(value=15, unit=ResolutionUnit.METERS),
            band_index=0,
            id="landsat:LC08:PRE:TOAR:alpha",
            name="alpha",
            file_index=0,
            display_range=[0, 1],
            data_range=[0, 1],
            jpx_layer_index=1,
            _saved=True,
        ),
        "nir": SpectralBand(
            wavelength_nm_max=878.85,
            data_type="UInt16",
            vendor_band_name="B5",
            id="landsat:LC08:PRE:TOAR:nir",
            file_index=1,
            wavelength_nm_center=864.7,
            jpx_layer_index=2,
            productid="landsat:LC08:PRE:TOAR",
            description="Near Infrared",
            tags=["spectral", "nir", "near-infrared", "30m", "landsat"],
            wavelength_nm_min=850.5500000000001,
            physical_range=[0.0, 1.0],
            band_index=0,
            vendor_order=5,
            sort_order=5,
            name="nir",
            display_range=[0, 10000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=28.3,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "cirrus": SpectralBand(
            wavelength_max=1375.0,
            data_type="UInt16",
            vendor_band_name="B9",
            id="landsat:LC08:PRE:TOAR:cirrus",
            file_index=1,
            wavelength_nm_center=1370,
            jpx_layer_index=3,
            product_id="landsat:LC08:PRE:TOAR",
            description="Cirrus",
            tags=["spectral", "cirrus", "30m", "landsat"],
            wavelength_nm_min=1365.0,
            physical_range=[0.0, 1.0],
            band_index=1,
            vendor_order=9,
            sort_order=9,
            name="cirrus",
            display_range=[0, 10000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=10,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "swir1": SpectralBand(
            wavelength_nm_max=1651.25,
            data_type="UInt16",
            vendor_band_name="B6",
            id="landsat:LC08:PRE:TOAR:swir1",
            file_index=1,
            wavelength_nm_center=1608.9,
            jpx_layer_index=2,
            product_id="landsat:LC08:PRE:TOAR",
            description="Short wave infrared 1",
            tags=["spectral", "swir", "swir1", "30m", "landsat"],
            wavelength_nm_min=1566.5500000000002,
            physical_range=[0.0, 1.0],
            band_index=1,
            vendor_order=6,
            sort_order=6,
            name="swir1",
            display_range=[0, 10000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=84.7,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "swir2": SpectralBand(
            wavelength_nm_max=2294.0499999999997,
            data_type="UInt16",
            vendor_band_name="B7",
            id="landsat:LC08:PRE:TOAR:swir2",
            file_index=1,
            wavelength_nm_center=2200.7,
            jpx_layer_index=2,
            product_id="landsat:LC08:PRE:TOAR",
            description="Short wave infrared 2",
            tags=["spectral", "swir", "swir2", "30m", "landsat"],
            wavelength_nm_min=2107.35,
            physical_range=[0.0, 1.0],
            band_index=2,
            vendor_order=7,
            sort_order=7,
            name="swir2",
            display_range=[0, 10000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=186.7,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "qa_cirrus": ClassBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Cirrus Classification",
            tags=["class", "cirrus", "30m", "landsat"],
            data_type="UInt16",
            vendor_band_name="qa_cirrus",
            band_index=2,
            id="landsat:LC08:PRE:TOAR:qa_cirrus",
            name="qa_cirrus",
            file_index=1,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            data_range=[0, 3],
            jpx_layer_index=1,
            display_range=[0, 3],
            _saved=True,
        ),
        "blue": SpectralBand(
            wavelength_nm_max=512.0,
            data_type="UInt16",
            vendor_band_name="B2",
            id="landsat:LC08:PRE:TOAR:blue",
            file_index=0,
            wavelength_nm_center=482,
            jpx_layer_index=0,
            product_id="landsat:LC08:PRE:TOAR",
            description="Blue, Pansharpened",
            tags=["spectral", "blue", "15m", "landsat"],
            wavelength_nm_min=452.0,
            physical_range=[0.0, 1.0],
            band_index=2,
            vendor_order=2,
            sort_order=2,
            name="blue",
            display_range=[0, 4000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=60,
            resolution=Resolution(value=15, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "bright-mask": MaskBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Bright Mask (blue > 20% reflective)",
            tags=["mask", "bright", "30m", "landsat"],
            data_type="UInt16",
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            band_index=2,
            id="landsat:LC08:PRE:TOAR:bright-mask",
            name="bright-mask",
            file_index=1,
            display_range=[0, 1],
            data_range=[0, 1],
            jpx_layer_index=0,
            _saved=True,
        ),
        "green": SpectralBand(
            wavelength_nm_max=590.05,
            data_type="UInt16",
            vendor_band_name="B3",
            id="landsat:LC08:PRE:TOAR:green",
            file_index=0,
            wavelength_nm_center=561.4,
            jpx_layer_index=0,
            product_id="landsat:LC08:PRE:TOAR",
            description="Green, Pansharpened",
            tags=["spectral", "green", "15m", "landsat"],
            wavelength_nm_min=532.75,
            physical_range=[0.0, 1.0],
            band_index=1,
            vendor_order=3,
            sort_order=3,
            name="green",
            display_range=[0, 4000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=57.3,
            resolution=Resolution(value=15, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "qa_snow": ClassBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Snow Classification",
            tags=["class", "snow", "30m", "landsat"],
            data_type="UInt16",
            vendor_band_name="qa_snow",
            band_index=1,
            id="landsat:LC08:PRE:TOAR:qa_snow",
            name="qa_snow",
            file_index=1,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            data_range=[0, 3],
            jpx_layer_index=1,
            display_range=[0, 3],
            _saved=True,
        ),
        "red": SpectralBand(
            wavelength_nm_max=673.35,
            data_type="UInt16",
            vendor_band_name="B4",
            id="landsat:LC08:PRE:TOAR:red",
            file_index=0,
            wavelength_nm_center=654.6,
            jpx_layer_index=0,
            product_id="landsat:LC08:PRE:TOAR",
            description="Red, Pansharpened",
            tags=["spectral", "red", "15m", "landsat"],
            wavelength_nm_min=635.85,
            physical_range=[0.0, 1.0],
            band_index=0,
            vendor_order=4,
            sort_order=4,
            name="red",
            display_range=[0, 4000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=37.5,
            resolution=Resolution(value=15, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "cloud-mask": MaskBand(
            product_id="landsat:LC08:PRE:TOAR",
            description="Binary Cloud Mask",
            tags=["mask", "cloud", "30m", "landsat"],
            data_type="UInt16",
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            band_index=1,
            id="landsat:LC08:PRE:TOAR:cloud-mask",
            name="cloud-mask",
            file_index=1,
            display_range=[0, 1],
            data_range=[0, 1],
            jpx_layer_index=0,
            _saved=True,
        ),
        "coastal-aerosol": SpectralBand(
            wavelength_nm_max=451.0,
            data_type="UInt16",
            vendor_band_name="B1",
            id="landsat:LC08:PRE:TOAR:coastal-aerosol",
            name="coastal-aerosol",
            file_index=1,
            wavelength_nm_center=443,
            jpx_layer_index=3,
            product_id="landsat:LC08:PRE:TOAR",
            description="Coastal Aerosol",
            tags=["spectral", "aerosol", "coastal", "30m", "landsat"],
            wavelength_nm_min=435.0,
            physical_range=[0.0, 1.0],
            band_index=0,
            vendor_order=1,
            sort_order=1,
            display_range=[0, 10000],
            data_range=[0, 10000],
            wavelength_nm_fwhm=16,
            resolution=Resolution(value=30, unit=ResolutionUnit.METERS),
            _saved=True,
        ),
        "derived:bai": DerivedBand(
            description="Burned Area Index",
            bands=["red", "nir"],
            data_range=[0, 65535],
            physical_range=[-1.0, 1.0],
            function_name="bai_uint16",
            data_type="UInt16",
            id="derived:bai",
            name="derived:bai",
            _saved=True,
        ),
        "derived:evi": DerivedBand(
            description="Enhanced Vegetation Index",
            bands=["blue", "red", "nir"],
            data_range=[0, 65535],
            physical_range=[-1.0, 1.0],
            function_name="evi_uint16",
            data_type="UInt16",
            id="derived:evi",
            name="derived:evi",
            _saved=True,
        ),
        "derived:ndvi": DerivedBand(
            description="Normalized Difference Vegetation Index",
            bands=["nir", "red"],
            data_range=[0, 65535],
            physical_range=[-1.0, 1.0],
            function_name="ndi_uint16",
            data_type="UInt16",
            id="derived:ndvi",
            name="derived:ndvi",
            _saved=True,
        ),
        "derived:ndwi": DerivedBand(
            description="Normalized Difference Water Index (with SWIR1)",
            bands=["nir", "swir1"],
            data_range=[0, 65535],
            physical_range=[-1.0, 1.0],
            function_name="ndi_uint16",
            data_type="UInt16",
            id="derived:ndwi",
            name="derived:ndwi",
            _saved=True,
        ),
        "derived:ndwi1": DerivedBand(
            description="Normalized Difference Water Index (with SWIR1)",
            bands=["nir", "swir1"],
            data_range=[0, 65535],
            physical_range=[-1.0, 1.0],
            function_name="ndi_uint16",
            data_type="UInt16",
            id="derived:ndwi1",
            name="derived:ndwi1",
            _saved=True,
        ),
        "derived:ndwi2": DerivedBand(
            description="Normalized Difference Water Index (with SWIR2)",
            bands=["nir", "swir2"],
            data_range=[0, 65535],
            physical_range=[-1.0, 1.0],
            function_name="ndi_uint16",
            data_type="UInt16",
            id="derived:ndwi2",
            name="derived:ndwi2",
            _saved=True,
        ),
        "derived:rsqrt": DerivedBand(
            description="SQRT of R",
            bands=["red"],
            data_range=[0, 1000],
            physical_range=[0, 1.0],
            function_name="sqrt",
            data_type="Float64",
            id="derived:rsqrt",
            name="derived:rsqrt",
            _saved=True,
        ),
        "derived:visual_cloud_mask": DerivedBand(
            description="Visual cloud mask based on grayness and green brightness",
            bands=["red", "green", "blue"],
            data_range=[0, 1],
            function_name="visual_cloud_mask",
            data_type="UInt16",
            id="derived:visual_cloud_mask",
            name="derived:visual_cloud_mask",
            _saved=True,
        ),
    },
    "modis:mod11a2:006": {
        "Clear_sky_days": SpectralBand(
            data_range=[0, 255],
            display_range=[1, 255],
            description="Day clear-sky coverage",
            data_type="Byte",
            id="modis:mod11a2:006:Clear_sky_days",
            jpx_layer_index=0,
            name="Clear_sky_days",
            vendor_band_name="Clear_sky_days",
            nodata=0,
            physical_range=[0.0, 255.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=8,
            file_index=1,
            vendor_order=11,
            sort_order=11,
            _saved=True,
        ),
        "Clear_sky_nights": SpectralBand(
            data_range=[0, 255],
            display_range=[1, 255],
            description="Night clear-sky coverage",
            data_type="Byte",
            id="modis:mod11a2:006:Clear_sky_nights",
            jpx_layer_index=0,
            name="Clear_sky_nights",
            vendor_band_name="Clear_sky_nights",
            nodata=0,
            physical_range=[0.0, 255.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=9,
            file_index=1,
            vendor_order=12,
            sort_order=12,
            _saved=True,
        ),
        "Day_view_angl": SpectralBand(
            data_range=[0, 255],
            display_range=[0, 130],
            description="View zenith angle of day observation",
            data_type="Byte",
            id="modis:mod11a2:006:Day_view_angl",
            jpx_layer_index=0,
            name="Day_view_angl",
            vendor_band_name="Day_view_angl",
            nodata=255,
            physical_range=[-65.0, 190.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=2,
            file_index=1,
            vendor_order=4,
            sort_order=4,
            _saved=True,
        ),
        "Day_view_time": SpectralBand(
            data_range=[0, 255],
            display_range=[0, 240],
            description="Local time of day observation",
            data_type="Byte",
            id="modis:mod11a2:006:Day_view_time",
            jpx_layer_index=0,
            name="Day_view_time",
            vendor_band_name="Day_view_time",
            nodata=255,
            physical_range=[0.0, 25.5],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=1,
            file_index=1,
            vendor_order=3,
            sort_order=3,
            _saved=True,
        ),
        "Emis_31": SpectralBand(
            data_range=[0, 255],
            display_range=[1, 255],
            description="Band 31 emissivity",
            data_type="Byte",
            id="modis:mod11a2:006:Emis_31",
            jpx_layer_index=0,
            name="Emis_31",
            vendor_band_name="Emis_31",
            nodata=255,
            physical_range=[0.49, 1.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=6,
            file_index=1,
            vendor_order=9,
            sort_order=9,
            _saved=True,
        ),
        "Emis_32": SpectralBand(
            data_range=[0, 255],
            display_range=[1, 255],
            description="Band 32 emissivity",
            data_type="Byte",
            id="modis:mod11a2:006:Emis_32",
            jpx_layer_index=0,
            name="Emis_32",
            vendor_band_name="Emis_32",
            nodata=255,
            physical_range=[0.49, 1.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=7,
            file_index=1,
            vendor_order=9,
            sort_order=9,
            _saved=True,
        ),
        "LST_Day_1km": SpectralBand(
            data_range=[0, 65535],
            display_range=[7500, 65535],
            description="Daytime Land Surface Temperature",
            data_type="UInt16",
            id="modis:mod11a2:006:LST_Day_1km",
            jpx_layer_index=0,
            name="LST_Day_1km",
            vendor_band_name="LST_Day_1km",
            nodata=0,
            physical_range=[0.0, 1310.7],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=0,
            file_index=0,
            vendor_order=1,
            sort_order=1,
            _saved=True,
        ),
        "LST_Night_1km": SpectralBand(
            data_range=[0, 65535],
            display_range=[7500, 65535],
            description="Nighttime Land Surface Temperature",
            data_type="UInt16",
            id="modis:mod11a2:006:LST_Night_1km",
            jpx_layer_index=0,
            name="LST_Night_1km",
            vendor_band_name="LST_Day_1km",
            nodata=0,
            physical_range=[0.0, 1310.7],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=2,
            file_index=0,
            vendor_order=5,
            sort_order=5,
            _saved=True,
        ),
        "Night_view_angl": SpectralBand(
            data_range=[0, 255],
            display_range=[0, 130],
            description="View zenith angle of night observation",
            data_type="Byte",
            id="modis:mod11a2:006:Night_view_angl",
            jpx_layer_index=0,
            name="Night_view_angl",
            vendor_band_name="Night_view_angl",
            nodata=255,
            physical_range=[-65.0, 190.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=5,
            file_index=1,
            vendor_order=8,
            sort_order=8,
            _saved=True,
        ),
        "Night_view_time": SpectralBand(
            data_range=[0, 255],
            display_range=[0, 240],
            description="Local time of night observation",
            data_type="Byte",
            id="modis:mod11a2:006:Night_view_time",
            jpx_layer_index=0,
            name="Night_view_time",
            vendor_band_name="Night_view_time",
            nodata=255,
            physical_range=[0.0, 25.5],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=4,
            file_index=1,
            vendor_order=7,
            sort_order=7,
            _saved=True,
        ),
        "QC_Day": ClassBand(
            data_range=[0, 255],
            default_range=[0, 255],
            description="Daytime LST Quality Indicators",
            data_type="Byte",
            id="modis:mod11a2:006:QC_Day",
            jpx_layer_index=0,
            name="QC_Day",
            vendor_band_name="QC_Day",
            physical_range=[0.0, 255.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=0,
            file_index=1,
            vendor_order=2,
            sort_order=2,
            _saved=True,
        ),
        "QC_Night": ClassBand(
            data_range=[0, 255],
            display_range=[0, 255],
            description="Nighttime LST Quality Indicators",
            data_type="Byte",
            id="modis:mod11a2:006:QC_Night",
            jpx_layer_index=0,
            name="QC_Night",
            vendor_band_name="QC_Night",
            physical_range=[0.0, 255.0],
            product_id="modis:mod11a2:006",
            resolution=Resolution(value=1000, unit=ResolutionUnit.METERS),
            band_index=3,
            file_index=1,
            vendor_order=6,
            sort_order=6,
            _saved=True,
        ),
    },
}


def _cached_bands_by_product(product, _client):
    return BANDS_BY_PRODUCT[product]


alpha = np.ones((122, 120), dtype="uint16")
alpha[2, 2] = 0

alpha1000 = np.ones((239, 235), dtype="uint16")
alpha1000[2, 2] = 0


RASTER = {
    '{"bands": ["nir", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="uint16"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "Int32", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="int32"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="uint16"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "Int32", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="int32"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "red", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 600}': (
        np.stack(
            [
                np.zeros((122, 120), dtype="uint16"),
                np.zeros((122, 120), dtype="uint16"),
                alpha,
            ]
        ),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 4000], "wavelength_max": 673.35, "data_unit": "TOAR", "wavelength_center": 654.6, "color": "Red", "dtype": "UInt16", "name_vendor": "B4", "id": "landsat:LC08:PRE:TOAR:red", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 635.85, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Red, Pansharpened", "tags": ["spectral", "red", "15m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "red", "vendor_order": 4, "name": "red", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 37.5, "nodata": null, "resolution": 15}, "band": 2, "colorInterpretation": "Red", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 3, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "red", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack(
            [
                np.zeros((122, 120), dtype="uint16"),
                np.zeros((122, 120), dtype="uint16"),
                alpha,
            ]
        ),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 4000], "wavelength_max": 673.35, "data_unit": "TOAR", "wavelength_center": 654.6, "color": "Red", "dtype": "UInt16", "name_vendor": "B4", "id": "landsat:LC08:PRE:TOAR:red", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 635.85, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Red, Pansharpened", "tags": ["spectral", "red", "15m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "red", "vendor_order": 4, "name": "red", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 37.5, "nodata": null, "resolution": 15}, "band": 2, "colorInterpretation": "Red", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 3, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 600}': (
        np.zeros((122, 120), dtype="uint16"),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"band": 1, "description": {"wavelength_max": 878.85, "data_unit_description": "Top of atmosphere reflectance", "data_unit": "TOAR", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "color": "Gray", "dtype": "UInt16", "wavelength_min": 850.55, "name_vendor": "B5", "product": "landsat:LC08:PRE:TOAR", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "id": "landsat:LC08:PRE:TOAR:nir", "vendor_order": 5, "nbits": 14, "type": "spectral", "name": "nir", "wavelength_center": 864.7, "data_range": [0, 10000], "resolution_unit": "m", "wavelength_unit": "nm", "resolution": 30, "wavelength_fwhm": 28.3, "nodata": null, "default_range": [0, 10000], "processing_level": "TOAR"}, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "Corder": "RPCL"}}, "size": [120, 122]}',
    ),  # noqa
    '{"bands": ["nir"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.zeros((122, 120), dtype="uint16"),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"band": 1, "description": {"wavelength_max": 878.85, "data_unit_description": "Top of atmosphere reflectance", "data_unit": "TOAR", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "color": "Gray", "dtype": "UInt16", "wavelength_min": 850.55, "name_vendor": "B5", "product": "landsat:LC08:PRE:TOAR", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "id": "landsat:LC08:PRE:TOAR:nir", "vendor_order": 5, "nbits": 14, "type": "spectral", "name": "nir", "wavelength_center": 864.7, "data_range": [0, 10000], "resolution_unit": "m", "wavelength_unit": "nm", "resolution": 30, "wavelength_fwhm": 28.3, "nodata": null, "default_range": [0, 10000], "processing_level": "TOAR"}, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1", "Corder": "RPCL"}}, "size": [120, 122]}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="uint16"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="uint16"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "*", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="uint16"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "*", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "alpha"], "data_type": "Int32", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="int32"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "*", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "red", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack(
            [
                np.zeros((122, 120), dtype="uint16"),
                np.zeros((122, 120), dtype="uint16"),
                alpha,
            ]
        ),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 4000], "wavelength_max": 673.35, "data_unit": "TOAR", "wavelength_center": 654.6, "color": "Red", "dtype": "UInt16", "name_vendor": "B4", "id": "landsat:LC08:PRE:TOAR:red", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 635.85, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Red, Pansharpened", "tags": ["spectral", "red", "15m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "red", "vendor_order": 4, "name": "red", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 37.5, "nodata": null, "resolution": 15}, "band": 2, "colorInterpretation": "Red", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 3, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "*", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["nir", "red"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack(
            [np.zeros((122, 120), dtype="uint16"), np.zeros((122, 120), dtype="uint16")]
        ),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 10000], "wavelength_max": 878.85, "data_unit": "TOAR", "wavelength_center": 864.7, "color": "Gray", "dtype": "UInt16", "name_vendor": "B5", "id": "landsat:LC08:PRE:TOAR:nir", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 850.55, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "vendor_order": 5, "name": "nir", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 28.3, "nodata": null, "resolution": 30}, "band": 1, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 4000], "wavelength_max": 673.35, "data_unit": "TOAR", "wavelength_center": 654.6, "color": "Red", "dtype": "UInt16", "name_vendor": "B4", "id": "landsat:LC08:PRE:TOAR:red", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 635.85, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Red, Pansharpened", "tags": ["spectral", "red", "15m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "red", "vendor_order": 4, "name": "red", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 37.5, "nodata": null, "resolution": 15}, "band": 2, "colorInterpretation": "Red", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 3, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "*", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["red", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([np.zeros((122, 120), dtype="uint16"), alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [0, 4000], "wavelength_max": 673.35, "data_unit": "TOAR", "wavelength_center": 654.6, "color": "Red", "dtype": "UInt16", "name_vendor": "B4", "id": "landsat:LC08:PRE:TOAR:red", "nbits": 14, "wavelength_unit": "nm", "wavelength_min": 635.85, "processing_level": "TOAR", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "Top of atmosphere reflectance", "description": "Red, Pansharpened", "tags": ["spectral", "red", "15m", "landsat"], "resolution_unit": "m", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "red", "vendor_order": 4, "name": "red", "type": "spectral", "data_range": [0, 10000], "wavelength_fwhm": 37.5, "nodata": null, "resolution": 15}, "band": 1, "colorInterpretation": "Red", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}, {"description": {"product": "landsat:LC08:PRE:TOAR", "nbits": 1, "description": "Alpha (valid data)", "data_description": "0: nodata, 1: valid data", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_range": [0, 1], "resolution": 15, "resolution_unit": "m", "data_unit_description": "unitless", "name_common": "alpha", "type": "mask", "nodata": null, "default_range": [0, 1], "id": "landsat:LC08:PRE:TOAR:alpha", "name": "alpha"}, "band": 2, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "size": [120, 122], "metadata": {"": {"id": "*", "Corder": "RPCL"}}}',
    ),  # noqa
    '{"bands": ["alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.stack([alpha]),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"band": 1, "description": {"product": "landsat:LC08:PRE:TOAR", "data_unit_description": "unitless", "description": "Alpha (valid data)", "tags": ["mask", "alpha", "15m", "landsat"], "color": "Alpha", "dtype": "UInt16", "data_description": "0: nodata, 1: valid data", "name_common": "alpha", "id": "landsat:LC08:PRE:TOAR:alpha", "nbits": 1, "name": "alpha", "type": "mask", "data_range": [0, 1], "resolution_unit": "m", "default_range": [0, 1], "nodata": null, "resolution": 15}, "colorInterpretation": "Alpha", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "1"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "metadata": {"": {"id": "*", "Corder": "RPCL"}}, "size": [120, 122]}',
    ),  # noqa
    '{"bands": ["nir"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"], "resolution": 600}': (
        np.zeros((122, 120), dtype="uint16"),
        '{"files": [], "cornerCoordinates": {"upperRight": [456219.441, 4580160.511], "lowerLeft": [384219.441, 4506960.511], "lowerRight": [456219.441, 4506960.511], "upperLeft": [384219.441, 4580160.511], "center": [420219.441, 4543560.511]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-94.3843159, 41.3646333], [-94.3705723, 40.7054251], [-93.5183204, 40.7123983], [-93.5235196, 41.3717692], [-94.3843159, 41.3646333]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"band": 1, "description": {"wavelength_max": 878.85, "data_unit_description": "Top of atmosphere reflectance", "data_unit": "TOAR", "description": "Near Infrared", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "color": "Gray", "dtype": "UInt16", "wavelength_min": 850.55, "name_vendor": "B5", "product": "landsat:LC08:PRE:TOAR", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "physical_range": [0.0, 1.0], "name_common": "nir", "id": "landsat:LC08:PRE:TOAR:nir", "vendor_order": 5, "nbits": 14, "type": "spectral", "name": "nir", "wavelength_center": 864.7, "data_range": [0, 10000], "resolution_unit": "m", "wavelength_unit": "nm", "resolution": 30, "wavelength_fwhm": 28.3, "nodata": null, "default_range": [0, 10000], "processing_level": "TOAR"}, "colorInterpretation": "Gray", "type": "UInt16", "block": [120, 1], "metadata": {"": {"NBITS": "14"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "geoTransform": [384219.440777, 600.0, 0.0, 4580160.51059, 0.0, -600.0], "metadata": {"": {"id": "*", "Corder": "RPCL"}}, "size": [120, 122]}',
    ),  # noqa
    '{"bands": ["red", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 1000}': (
        np.stack([np.zeros((239, 235), dtype="uint16"), alpha1000]),
        '{"metadata": {"": {"Corder": "RPCL", "id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"}}, "driverShortName": "MEM", "wgs84Extent": {"coordinates": [[[-95.9559596, 42.8041728], [-95.8589268, 40.654253], [-93.0793836, 40.6896344], [-93.0820826, 42.8423136], [-95.9559596, 42.8041728]]], "type": "Polygon"}, "geoTransform": [258292.5, 1000.0, 0.0, 4743307.5, 0.0, -1000.0], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "cornerCoordinates": {"upperRight": [493292.5, 4743307.5], "center": [375792.5, 4623807.5], "upperLeft": [258292.5, 4743307.5], "lowerRight": [493292.5, 4504307.5], "lowerLeft": [258292.5, 4504307.5]}, "files": [], "bands": [{"block": [235, 1], "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:red", "processing_level": "TOAR", "description": "Red, Pansharpened", "resolution_unit": "m", "wavelength_max": 673.35, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Red", "name_common": "red", "name": "red", "name_vendor": "B4", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 635.85, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "red", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 4, "resolution": 15, "wavelength_center": 654.6, "wavelength_fwhm": 37.5}, "band": 1, "colorInterpretation": "Red"}, {"block": [235, 1], "metadata": {"": {"NBITS": "1"}}, "type": "UInt16", "description": {"nodata": null, "color": "Alpha", "type": "mask", "id": "landsat:LC08:PRE:TOAR:alpha", "description": "Alpha (valid data)", "resolution_unit": "m", "data_description": "0: nodata, 1: valid data", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "unitless", "nbits": 1, "tags": ["mask", "alpha", "15m", "landsat"], "name_common": "alpha", "default_range": [0, 1], "name": "alpha", "data_range": [0, 1], "dtype": "UInt16", "resolution": 15}, "band": 2, "colorInterpretation": "Alpha"}], "size": [235, 239], "driverLongName": "In Memory Raster"}',
    ),  # noqa
    '{"bands": ["red", "green"], "data_type": "Int32", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 600}': (
        np.stack(
            [np.zeros((122, 120), dtype="int32"), np.zeros((122, 120), dtype="int32")]
        ),
        '{"metadata": {"": {"Corder": "RPCL", "id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"}}, "driverShortName": "MEM", "wgs84Extent": {"coordinates": [[[-95.9559596, 42.8041728], [-95.8589268, 40.654253], [-93.0793836, 40.6896344], [-93.0820826, 42.8423136], [-95.9559596, 42.8041728]]], "type": "Polygon"}, "geoTransform": [258292.5, 1000.0, 0.0, 4743307.5, 0.0, -1000.0], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "cornerCoordinates": {"upperRight": [493292.5, 4743307.5], "center": [375792.5, 4623807.5], "upperLeft": [258292.5, 4743307.5], "lowerRight": [493292.5, 4504307.5], "lowerLeft": [258292.5, 4504307.5]}, "files": [], "bands": [{"block": [235, 1], "mask": {"flags": ["PER_DATASET", "ALPHA"], "overviews": []}, "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:red", "processing_level": "TOAR", "description": "Red, Pansharpened", "resolution_unit": "m", "wavelength_max": 673.35, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Red", "name_common": "red", "name": "red", "name_vendor": "B4", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 635.85, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "red", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 4, "resolution": 15, "wavelength_center": 654.6, "wavelength_fwhm": 37.5}, "band": 1, "colorInterpretation": "Red"}, {"block": [235, 1], "mask": {"flags": ["PER_DATASET", "ALPHA"], "overviews": []}, "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:green", "processing_level": "TOAR", "description": "Green, Pansharpened", "resolution_unit": "m", "wavelength_max": 590.05, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Green", "name_common": "green", "name": "green", "name_vendor": "B3", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 532.75, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "green", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 3, "resolution": 15, "wavelength_center": 561.4, "wavelength_fwhm": 57.3}, "band": 2, "colorInterpretation": "Green"}], "size": [235, 239], "driverLongName": "In Memory Raster"}',
    ),  # noqa
    '{"bands": ["red", "green", "blue", "alpha"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 1000}': (
        np.stack(
            [
                np.zeros((239, 235), dtype="uint16"),
                np.zeros((239, 235), dtype="uint16"),
                np.zeros((239, 235), dtype="uint16"),
                alpha1000,
            ]
        ),
        '{"metadata": {"": {"Corder": "RPCL", "id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"}}, "driverShortName": "MEM", "wgs84Extent": {"coordinates": [[[-95.9559596, 42.8041728], [-95.8589268, 40.654253], [-93.0793836, 40.6896344], [-93.0820826, 42.8423136], [-95.9559596, 42.8041728]]], "type": "Polygon"}, "geoTransform": [258292.5, 1000.0, 0.0, 4743307.5, 0.0, -1000.0], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "cornerCoordinates": {"upperRight": [493292.5, 4743307.5], "center": [375792.5, 4623807.5], "upperLeft": [258292.5, 4743307.5], "lowerRight": [493292.5, 4504307.5], "lowerLeft": [258292.5, 4504307.5]}, "files": [], "bands": [{"block": [235, 1], "mask": {"flags": ["PER_DATASET", "ALPHA"], "overviews": []}, "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:red", "processing_level": "TOAR", "description": "Red, Pansharpened", "resolution_unit": "m", "wavelength_max": 673.35, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Red", "name_common": "red", "name": "red", "name_vendor": "B4", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 635.85, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "red", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 4, "resolution": 15, "wavelength_center": 654.6, "wavelength_fwhm": 37.5}, "band": 1, "colorInterpretation": "Red"}, {"block": [235, 1], "mask": {"flags": ["PER_DATASET", "ALPHA"], "overviews": []}, "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:green", "processing_level": "TOAR", "description": "Green, Pansharpened", "resolution_unit": "m", "wavelength_max": 590.05, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Green", "name_common": "green", "name": "green", "name_vendor": "B3", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 532.75, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "green", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 3, "resolution": 15, "wavelength_center": 561.4, "wavelength_fwhm": 57.3}, "band": 2, "colorInterpretation": "Green"}, {"block": [235, 1], "mask": {"flags": ["PER_DATASET", "ALPHA"], "overviews": []}, "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:blue", "processing_level": "TOAR", "description": "Blue, Pansharpened", "resolution_unit": "m", "wavelength_max": 512.0, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Blue", "name_common": "blue", "name": "blue", "name_vendor": "B2", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 452.0, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "blue", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 2, "resolution": 15, "wavelength_center": 482, "wavelength_fwhm": 60}, "band": 3, "colorInterpretation": "Blue"}, {"block": [235, 1], "metadata": {"": {"NBITS": "1"}}, "type": "UInt16", "description": {"nodata": null, "color": "Alpha", "type": "mask", "id": "landsat:LC08:PRE:TOAR:alpha", "description": "Alpha (valid data)", "resolution_unit": "m", "data_description": "0: nodata, 1: valid data", "product": "landsat:LC08:PRE:TOAR", "data_unit_description": "unitless", "nbits": 1, "tags": ["mask", "alpha", "15m", "landsat"], "name_common": "alpha", "default_range": [0, 1], "name": "alpha", "data_range": [0, 1], "dtype": "UInt16", "resolution": 15}, "band": 4, "colorInterpretation": "Alpha"}], "size": [235, 239], "driverLongName": "In Memory Raster"}',
    ),  # noqa
    '{"bands": ["red", "nir"], "data_type": "UInt16", "inputs": ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"], "resolution": 1000}': (
        np.stack(
            [np.zeros((239, 235), dtype="uint16"), np.zeros((239, 235), dtype="uint16")]
        ),
        '{"metadata": {"": {"Corder": "RPCL", "id": "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"}}, "driverShortName": "MEM", "wgs84Extent": {"coordinates": [[[-95.9559596, 42.8041728], [-95.8589268, 40.654253], [-93.0793836, 40.6896344], [-93.0820826, 42.8423136], [-95.9559596, 42.8041728]]], "type": "Polygon"}, "geoTransform": [258292.5, 1000.0, 0.0, 4743307.5, 0.0, -1000.0], "coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 15N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-93],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32615\\"]]"}, "cornerCoordinates": {"upperRight": [493292.5, 4743307.5], "center": [375792.5, 4623807.5], "upperLeft": [258292.5, 4743307.5], "lowerRight": [493292.5, 4504307.5], "lowerLeft": [258292.5, 4504307.5]}, "files": [], "bands": [{"block": [235, 1], "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:red", "processing_level": "TOAR", "description": "Red, Pansharpened", "resolution_unit": "m", "wavelength_max": 673.35, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Red", "name_common": "red", "name": "red", "name_vendor": "B4", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 635.85, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "red", "15m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 4000], "vendor_order": 4, "resolution": 15, "wavelength_center": 654.6, "wavelength_fwhm": 37.5}, "band": 1, "colorInterpretation": "Red"}, {"block": [235, 1], "metadata": {"": {"NBITS": "14"}}, "type": "UInt16", "description": {"nodata": null, "data_unit_description": "Top of atmosphere reflectance", "id": "landsat:LC08:PRE:TOAR:nir", "processing_level": "TOAR", "description": "Near Infrared", "resolution_unit": "m", "wavelength_max": 878.85, "product": "landsat:LC08:PRE:TOAR", "nbits": 14, "wavelength_unit": "nm", "color": "Gray", "name_common": "nir", "name": "nir", "name_vendor": "B5", "data_unit": "TOAR", "data_range": [0, 10000], "wavelength_min": 850.55, "type": "spectral", "dtype": "UInt16", "data_description": "TOAR, 0-10000 is 0 - 100% reflective", "tags": ["spectral", "nir", "near-infrared", "30m", "landsat"], "physical_range": [0.0, 1.0], "default_range": [0, 10000], "vendor_order": 5, "resolution": 30, "wavelength_center": 864.7, "wavelength_fwhm": 28.3}, "band": 2, "colorInterpretation": "Gray"}], "size": [235, 239], "driverLongName": "In Memory Raster"}',
    ),  # noqa
    '{"bands": ["Clear_sky_days", "Clear_sky_nights"], "data_type": "Byte", "inputs": ["modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"], "resolution": 1000}': (
        np.stack(
            [np.zeros((688, 473), dtype="uint16"), np.zeros((688, 473), dtype="uint16")]
        ),
        '{"files": [],"cornerCoordinates": {"upperRight": [340252.341, 6855234.987], "lowerLeft": [298972.341, 6826854.987], "lowerRight": [340252.341, 6826854.987], "upperLeft": [298972.341, 6855234.987], "center": [319612.341, 6841044.987]},"wgs84Extent": {"type": "Polygon", "coordinates": [[[-144.8118058, 61.7770149],   [-144.7805921, 61.5228056],   [-144.0056918, 61.5420874],   [-144.0305346, 61.7965016],   [-144.8118058, 61.7770149]]]},"driverShortName": "MEM","driverLongName": "In Memory Raster","bands": [{"description": {"default_range": [0, 4000],   "wavelength_max": 680.0,   "data_unit": "TOAR",   "color": "Red",   "dtype": "UInt16",   "name_vendor": "B4",   "type": "spectral",   "id": "sentinel-2:L1C:red",   "nbits": 14,   "wavelength_unit": "nm",   "wavelength_min": 650.0,   "processing_level": "TOAR",   "product": "sentinel-2:L1C",   "data_unit_description": "Top of atmosphere reflectance",   "description": "Red",   "tags": ["spectral", "red", "10m", "sentinel-2"],   "resolution_unit": "m",   "vendor_order": 4,   "physical_range": [0.0, 1.0],   "name_common": "red",   "data_description": "TOAR, 0-10000 is 0 - 100% reflective",   "name": "red",   "wavelength_center": 665,   "data_range": [0, 10000],   "wavelength_fwhm": 30,     "nodata": null,   "resolution": 10},  "mask": {"overviews": [], "flags": ["PER_DATASET", "ALPHA"]},  "band": 1,  "colorInterpretation": "Red",  "type": "Byte",  "block": [688, 1],  "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 4000],   "wavelength_max": 577.5,   "data_unit": "TOAR",   "color": "Green",   "dtype": "UInt16",   "name_vendor": "B3",   "type": "spectral",   "id": "sentinel-2:L1C:green",   "nbits": 14,   "wavelength_unit": "nm",   "wavelength_min": 542.5,   "processing_level": "TOAR",   "product": "sentinel-2:L1C",   "data_unit_description": "Top of atmosphere reflectance",   "description": "Green",   "tags": ["spectral", "green", "10m", "sentinel-2"],   "resolution_unit": "m",   "vendor_order": 3,   "physical_range": [0.0, 1.0],   "name_common": "green",   "data_description": "TOAR, 0-10000 is 0 - 100% reflective",   "name": "green",   "wavelength_center": 560,   "data_range": [0, 10000],   "wavelength_fwhm": 35,     "nodata": null,   "resolution": 10},  "mask": {"overviews": [], "flags": ["PER_DATASET", "ALPHA"]},  "band": 2,  "colorInterpretation": "Green",  "type": "Byte",  "block": [688, 1],  "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 4000],   "wavelength_max": 522.5,   "data_unit": "TOAR",   "color": "Blue",   "dtype": "UInt16",   "name_vendor": "B2",   "type": "spectral",   "id": "sentinel-2:L1C:blue",   "nbits": 14,   "wavelength_unit": "nm",   "wavelength_min": 457.5,   "processing_level": "TOAR",   "product": "sentinel-2:L1C",   "data_unit_description": "Top of atmosphere reflectance",   "description": "Blue",   "tags": ["spectral", "blue", "10m", "sentinel-2"],   "resolution_unit": "m",   "vendor_order": 2,   "physical_range": [0.0, 1.0],   "name_common": "blue",   "data_description": "TOAR, 0-10000 is 0 - 100% reflective",   "name": "blue",   "wavelength_center": 490,   "data_range": [0, 10000],   "wavelength_fwhm": 65,     "nodata": null,   "resolution": 10},  "mask": {"overviews": [], "flags": ["PER_DATASET", "ALPHA"]},  "band": 3,  "colorInterpretation": "Blue",  "type": "Byte",  "block": [688, 1],  "metadata": {"": {"NBITS": "14"}}}, {"description": {"default_range": [0, 1],   "product": "sentinel-2:L1C",   "nbits": 1,   "description": "Alpha (valid data)",   "tags": ["mask", "alpha", "10m", "sentinel-2"],   "color": "Alpha",   "dtype": "UInt16",   "data_range": [0, 1],   "resolution": 10,   "name": "alpha",   "resolution_unit": "m",   "data_unit_description": "unitless",   "name_common": "alpha",     "nodata": null,   "type": "mask",   "id": "sentinel-2:L1C:alpha",   "data_description": "0: nodata, 1: valid data"},  "band": 4,  "colorInterpretation": "Alpha",  "type": "Byte",  "block": [688, 1],  "metadata": {"": {"NBITS": "1"}}}],"coordinateSystem": {"wkt": "PROJCS[\\"WGS 84 / UTM zone 7N\\",\\n    GEOGCS[\\"WGS 84\\",\\n        DATUM[\\"WGS_1984\\",\\n            SPHEROID[\\"WGS 84\\",6378137,298.257223563,\\n                AUTHORITY[\\"EPSG\\",\\"7030\\"]],\\n            AUTHORITY[\\"EPSG\\",\\"6326\\"]],\\n        PRIMEM[\\"Greenwich\\",0,\\n            AUTHORITY[\\"EPSG\\",\\"8901\\"]],\\n        UNIT[\\"degree\\",0.0174532925199433,\\n            AUTHORITY[\\"EPSG\\",\\"9122\\"]],\\n        AUTHORITY[\\"EPSG\\",\\"4326\\"]],\\n    PROJECTION[\\"Transverse_Mercator\\"],\\n    PARAMETER[\\"latitude_of_origin\\",0],\\n    PARAMETER[\\"central_meridian\\",-141],\\n    PARAMETER[\\"scale_factor\\",0.9996],\\n    PARAMETER[\\"false_easting\\",500000],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"metre\\",1,\\n        AUTHORITY[\\"EPSG\\",\\"9001\\"]],\\n    AXIS[\\"Easting\\",EAST],\\n    AXIS[\\"Northing\\",NORTH],\\n    AUTHORITY[\\"EPSG\\",\\"32607\\"]]"},"geoTransform": [298972.341031, 60.0, 0.0, 6855234.98696, 0.0, -60.0],"metadata": {"": {"id": "sentinel-2:L1C:2017-08-07_07VCJ_99_S2A_v1",  "Corder": "RPCL"}},"size": [688, 473]}',
    ),  # noqa
    '{"bands": ["Clear_sky_days", "Clear_sky_nights"], "data_type": "Byte", "inputs": ["modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1", "modis:mod11a2:006:meta_MOD11A2.A2000049.h08v05.006.2015058135046_v1"], "resolution": 600}': (
        np.stack(
            [
                np.zeros((1853, 3707), dtype="uint16"),
                np.zeros((1853, 3707), dtype="uint16"),
            ]
        ),
        '{"files": [], "cornerCoordinates": {"upperRight": [-8895305.198, 4447802.079], "lowerLeft": [-11119505.198, 3336002.079], "lowerRight": [-8895305.198, 3336002.079], "upperLeft": [-11119505.198, 4447802.079], "center": [-10007405.198, 3891902.079]}, "wgs84Extent": {"type": "Polygon", "coordinates": [[[-130.5407289, 40.0], [-115.4716289, 30.0013537], [-92.3741986, 30.0013537], [-104.4290734, 40.0], [-130.5407289, 40.0]]]}, "driverShortName": "MEM", "driverLongName": "In Memory Raster", "bands": [{"description": {"default_range": [1, 255], "product": "modis:mod11a2:006", "vendor_order": 11, "data_unit": "unitless", "description": "Day clear-sky coverage", "resolution_unit": "meters", "dtype": "Byte", "physical_range": [0.0, 255.0], "data_range": [0, 255], "name_vendor": "Clear_sky_days", "nbits": 8, "type": "spectral", "nodata": 0, "resolution": 1000, "id": "modis:mod11a2:006:Clear_sky_days", "name": "Clear_sky_days"}, "noDataValue": 0.0, "band": 1, "colorInterpretation": "Undefined", "type": "Byte", "block": [3707, 1], "metadata": {"": {"NBITS": "8"}}}, {"description": {"default_range": [1, 255], "product": "modis:mod11a2:006", "vendor_order": 12, "data_unit": "unitless", "description": "Night clear-sky coverage", "resolution_unit": "meters", "dtype": "Byte", "physical_range": [0.0, 255.0], "data_range": [0, 255], "name_vendor": "Clear_sky_nights", "nbits": 8, "type": "spectral", "nodata": 0, "resolution": 1000, "id": "modis:mod11a2:006:Clear_sky_nights", "name": "Clear_sky_nights"}, "noDataValue": 0.0, "band": 2, "colorInterpretation": "Undefined", "type": "Byte", "block": [3707, 1], "metadata": {"": {"NBITS": "8"}}}], "coordinateSystem": {"wkt": "PROJCS[\\"unnamed\\",\\n    GEOGCS[\\"unnamed ellipse\\",\\n        DATUM[\\"unknown\\",\\n            SPHEROID[\\"unnamed\\",6371007.181,0]],\\n        PRIMEM[\\"Greenwich\\",0],\\n        UNIT[\\"degree\\",0.0174532925199433]],\\n    PROJECTION[\\"Sinusoidal\\"],\\n    PARAMETER[\\"longitude_of_center\\",0],\\n    PARAMETER[\\"false_easting\\",0],\\n    PARAMETER[\\"false_northing\\",0],\\n    UNIT[\\"Meter\\",1]]"}, "geoTransform": [-11119505.197665, 600.0, 0.0, 4447802.079066, 0.0, -600.0], "metadata": {"": {"id": "*", "Corder": "RPCL"}}, "size": [3707, 1853]}',
    ),  # noqa
}


def _raster_ndarray(self, **kwargs):
    a, meta = RASTER[
        json.dumps(
            {k: kwargs[k] for k in ("bands", "data_type", "inputs", "resolution")},
            sort_keys=True,
        )
    ]

    if kwargs.get("masked", True):
        if not np.ma.is_masked(a):
            mask = np.zeros(a.shape)
            if kwargs.get("mask_alpha") and kwargs["bands"][-1] == "alpha":
                mask[:] = ~(a[-1].astype(bool))
            a = np.ma.array(a, mask=mask)
    else:
        if np.ma.is_masked(a):
            a = a.data

    if kwargs.get("drop_alpha", False):
        a = a[:-1]

    return a, json.loads(meta)
