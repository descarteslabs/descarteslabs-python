Metadata
--------

.. automodule:: descarteslabs.client.services.metadata
    :members:


Filtering
---------

.. py:attribute:: descarteslabs.client.services.metadata.properties

    Most of the metadata searching functions allow for flexible filtering on scene metadata.

        >>> from descarteslabs.client.services.metadata import Metadata, properties as p
        >>> metadata = Metadata()

        >>> metadata.search(products=["sentinel-2:L1C"], q=[p.acquired > "2017", p.cloud_fraction < 0.25])
        {'bytes': 28174123929918, 'count': 330034, 'products': ['sentinel-2:L1C']}

        >>> metadata.summary(products=["sentinel-2:L1C"], q=[p.acquired > "2017", p.cloud_fraction < 0.5, 150 < p.azimuth_angle < 160])
        {'bytes': 747678539408, 'count': 5979, 'products': ['sentinel-2:L1C']}

        >>> metadata.summary(products=['modis:09:CREFL'], q=[p.sat_id == 'Aqua'])
        {'bytes': 68425075256895, 'count': 7162652, 'products': ['modis:09:CREFL']}

        >>> metadata.summary(products=['modis:09:CREFL'], q=[p.sat_id == 'Terra'])
        {'bytes': 71211948012243, 'count': 7421542, 'products': ['modis:09:CREFL']}

    :var absolute_orbit:

        Number of orbits elapsed since the first ascending node crossing after launch.

        Only present for Sentinel 2 imagery.

    :var acquired: Acquisition date and time.
    :var archived: Archival date and time.
    :var area: Area of the captured scene in square kilometers.
    :var azimuth_angle: Satellite azimuth angle in degrees.
    :var cirrus_fraction: Fraction of pixels identified as cirrus clouds. Only applicable to Sentinel-2 data.

        See https://sentinel.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-1c/cloud-masks for more information.

    :var cloud_fraction: Fraction of cloudy pixels determined by the vendor cloud mask.
    :var cloud_fraction_0: Fraction of cloudy pixels supplied by the vendor.
    :var earth_sun_distance: Earth-sun distance in astronomical units.
    :var geolocation_accuracy:

        RMSE of the geometric residuals (pixels) in both line and sample directions measured on the terrain-corrected product independently using GLS2000. Only applicable to Landsat data.
    :var gsd: Ground sampling distance in meters.
    :var opaque_fraction: Fraction of pixels identified as dense clouds. Only applicable to Sentinel-2 data.

        See https://sentinel.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-1c/cloud-masks for more information.

    :var product: Product identifier (e.g. ``landsat:LC08:PRE:TOAR``)
    :var processed: Timestamp of when the scene was processed into the platform.
    :var published: Date and time when the scene was published.
    :var relative_orbit:

        Count of orbits from 1 to the number contained in a repeat cycle. Relative orbit number 1 corresponds to the orbit whose ascending node crossing is closest to the Greenwich Meridian (eastwards).

        Only present for Sentinel 1 and 2 imagery.

    :var roll_angle: Satellite zenith angle for Landsat 8 imagery.
    :var sat_id: Vendor-specific satellite ID.
    :var solar_azimuth_angle: Solar azimuth angle when the scene was captured.
    :var solar_elevation_angle: Solar elevation angle when the scene was captured.
    :var terrain_correction: Landsat Level-1 data processing level

        * **L1T** *(Pre-Collection)* / **L1TP** *(Collection 1)* - Radiometrically calibrated and orthorectified using
          ground control points and DEM data to correct for relief displacement. These are the highest quality
          Level-1 products suitable for pixel-level time series analysis.

        * **L1GT** *(Pre-Collection)* / **L1GT** *(Collection 1)* - Radiometrically calibrated and with systematic
          geometric corrections applied using the spacecraft ephemeris data and DEM data to correct for relief displacement.

        * **L1G** *(Pre-Collection)* / **L1GS** *(Collection 1)* - Radiometrically calibrated and with only systematic
          geometric corrections applied using the spacecraft ephemeris data.


        See https://landsat.usgs.gov/landsat-processing-details/ for more information.

    :var tile_id: Vendor-specific tile ID.
    :var view_angle: Satellite view angle in degrees.
