
Getting Started
===============

Find a place, search for imagery, and rasterize it.

This tutorial demonstrates a few of the basic apis in the Descartes Labs
Platform. We'll start out by figuring out where we want to look. This
will utilize the ``Places`` functionality. From there, we'll search for
available imagery through our ``Metadata`` api. Finally, we'll rasterize
the available imagery into a ``numpy`` array and display it using
``matplotlib``.

.. code:: ipython2

    import os
    import warnings
    from pprint import pprint
    # Instead of using the `descarteslabs login` command, you can manually set the token information.
    # Probably not wise to then share this notebooks with others.
    # os.environ['CLIENT_ID'] = '<YOUR ID HERE>'
    # os.environ['CLIENT_SECRET'] = '<YOUR SECRET HERE>'

.. code:: ipython2

    import descarteslabs as dl

So you now have access to a giant archive of imagery. First question is
– where do you want to look? You might answer this question in many
ways, but one of the ways we can help is by providing mechanisms to find
shapes of known places. Our ``Places`` api has a ``find`` method that
does fuzzy-matching searches of places. As an example, let’s try to find
Taos, New Mexico (a favorite place for Cartesians to go hiking, biking,
camping, and skiing).

.. code:: ipython2

    # Find potential matches
    matches = dl.places.find('new-mexico_taos')
    pprint(matches)
    # The first one looks good to me, so lets make that our area of interest.
    aoi = matches[0]


.. parsed-literal::

    [{u'bbox': [-106.058364, 36.013014, -105.200117, 36.995841],
      u'id': 102081181,
      u'name': u'Taos',
      u'path': u'continent:north-america_country:united-states_region:new-mexico_district:northwest_county:taos',
      u'placetype': u'county',
      u'slug': u'north-america_united-states_new-mexico_northwest_taos'}]


.. code:: ipython2

    # This area of interest just gives us some basic properties such as bounding boxes.
    # To access a GeoJSON Geometry object of that place, we call the `Places.shape` method, in this case
    # accessing a low-resolution version of this particular shape.
    shape = dl.places.shape(aoi['slug'], geom='low')

.. code:: ipython2

    # If you'd like, load up some libraries like matplotlib, shapley, and cartopy,
    # and use them to plot Taos county.
    #%matplotlib inline
    %matplotlib inline
    import matplotlib.pyplot as plt
    import shapely.geometry
    import cartopy
    
    bbox = shape['bbox']
    
    # Lets load up the Albers Equal Area projection.
    lonlat_crs = cartopy.crs.PlateCarree()
    albers = cartopy.crs.AlbersEqualArea(central_latitude=36.0, central_longitude=-105)
    
    fig = plt.figure(figsize=(4, 8))
    ax = plt.subplot(projection=albers) # Specify projection of the map here
    shp = shapely.geometry.shape(shape['geometry'])
    
    # When adding a geometry in latlon coordinates, specify the latlon projection
    ax.add_geometries([shp], lonlat_crs)
    
    # You can set extents in latlon, as long as you specify the projection with `crs`
    ax.set_extent((bbox[0], bbox[2], bbox[1], bbox[3]), crs=lonlat_crs)
    ax.gridlines(crs=lonlat_crs)
    plt.show()


.. parsed-literal::

    /home/skillman/local/src/descarteslabs-python/env/local/lib/python2.7/site-packages/matplotlib/ticker.py:1685: UserWarning: Steps argument should be a sequence of numbers
    increasing from 1 to 10, inclusive. Behavior with
    values outside this range is undefined, and will
    raise a ValueError in future versions of mpl.
      warnings.warn('Steps argument should be a sequence of numbers\n'



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/getting_started_files/getting_started_6_1.png


Searching for available imagery
-------------------------------

.. code:: ipython2

    # What imagery is available?
    sources = dl.metadata.sources()
    pprint(sources)


.. parsed-literal::

    [{u'product': u'modis:09:CREFL', u'sat_id': u'Terra'},
     {u'product': u'modis:09:CREFL', u'sat_id': u'Aqua'},
     {u'product': u'landsat:LE07:PRE:TOAR', u'sat_id': u'LANDSAT_7'},
     {u'product': u'landsat:LE07:PRE:TOAR', u'sat_id': u'Landsat7'},
     {u'product': u'sentinel-2:L1C', u'sat_id': u'S2A'},
     {u'product': u'landsat:LT05:PRE:TOAR', u'sat_id': u'LANDSAT_5'},
     {u'product': u'landsat:LT05:PRE:TOAR', u'sat_id': u'Landsat5'},
     {u'product': u'sentinel-3:OLCI_RGB', u'sat_id': u'S3A'},
     {u'product': u'landsat:LC08:PRE:TOAR', u'sat_id': u'LANDSAT_8'},
     {u'product': u'usda:naip:rgbn', u'sat_id': u'NAIP'},
     {u'product': u'landsat:LC08:PRE:LaSRC', u'sat_id': u'L8SR'},
     {u'product': u'sentinel-1:GRD', u'sat_id': u'SENTINEL-1A'},
     {u'product': u'sentinel-1:GRD', u'sat_id': u'SENTINEL-1B'},
     {u'product': u'srtm:GL1003', u'sat_id': u'srtm'},
     {u'product': u'landsat:LT04:PRE:TOAR', u'sat_id': u'Landsat4'},
     {u'product': u'landsat:LT04:PRE:TOAR', u'sat_id': u'LANDSAT_4'},
     {u'product': u'usda:cdl', u'sat_id': u'CDL'}]


Lets find some Landsat 8 imagery over our AOI
---------------------------------------------

Here we'll use the ``Metadata`` api to search for available imagery over
a spatio-temporal extent. In this case we'll specify that we're
interested in our aoi using its slug, and the last few weeks of March,
2017.

.. code:: ipython2

    import json
    feature_collection = dl.metadata.search(products='landsat:LC08:PRE:TOAR', start_time='2017-03-12', 
                                            end_time='2017-03-20', limit=10, place=aoi['slug'])
    # As the variable name implies, this returns a FeatureCollection GeoJSON dictionary.
    # Its 'features' are the available scenes.
    print len(feature_collection['features'])
    # The 'id' associated with each feature is a unique identifier into our imagery database.
    # In this case there are two L8 scenes from adjoining WRS rows.
    print [f['id'] for f in feature_collection['features']]


.. parsed-literal::

    2
    [u'landsat:LC08:PRE:TOAR:meta_LC80330342017072_v1', u'landsat:LC08:PRE:TOAR:meta_LC80330352017072_v1']


.. code:: ipython2

    # Lets look at the first feature. The features are a FeatureCollection,
    # so if we print it out we get a bunch of information. For example, we see that 
    # the overall cloud fraction is 1.2%.:
    f0 = feature_collection['features'][0]
    pprint(f0)


.. parsed-literal::

    {u'geometry': {u'coordinates': [[[-105.8898318, 38.5103536],
                                     [-103.8445201, 38.1129959],
                                     [-104.3838427, 36.4246539],
                                     [-106.377808, 36.8201224],
                                     [-105.8898318, 38.5103536]]],
                   u'type': u'Polygon'},
     u'id': u'landsat:LC08:PRE:TOAR:meta_LC80330342017072_v1',
     u'key': u'meta_LC80330342017072_v1',
     u'properties': {u'acquired': u'2017-03-13T17:37:50.132812Z',
                     u'area': 35462.0,
                     u'bits_per_pixel': [1.463, 1.778, 0.922],
                     u'bright_fraction': 0.1168,
                     u'bucket': u'gs://descartes-l8/',
                     u'cloud_fraction': 0.1907,
                     u'cloud_fraction_0': 0.0643,
                     u'cs_code': u'EPSG:32613',
                     u'descartes_version': u'hedj-landsat-0.9.7.4',
                     u'file_md5s': [u'e084f11d78cedb76968959231e598112',
                                    u'd8970e1688a8a97e09f9f4eace81edf6'],
                     u'file_sizes': [86541838, 46913535],
                     u'files': [u'2017-03-13_033034_L8_432.jp2',
                                u'2017-03-13_033034_L8_567_19a.jp2'],
                     u'fill_fraction': 0.6487,
                     u'geolocation_accuracy': 4.245,
                     u'geotrans': [373192.5, 15.0, 0.0, 4265107.5, 0.0, -15.0],
                     u'identifier': u'LC80330342017072LGN00.tar.bz',
                     u'key': u'meta_LC80330342017072_v1',
                     u'processed': 1489695759,
                     u'product': u'landsat:LC08:PRE:TOAR',
                     u'projcs': u'WGS 84 / UTM zone 13N',
                     u'published': u'2017-03-13T23:47:46Z',
                     u'raster_size': [15440, 15736],
                     u'reflectance_scale': [0.2182,
                                            0.214,
                                            0.2337,
                                            0.276,
                                            0.4548,
                                            1.8054,
                                            5.5498,
                                            1.1066,
                                            0.245],
                     u'roll_angle': -0.001,
                     u'sat_id': u'LANDSAT_8',
                     u'solar_azimuth_angle': 146.78826655,
                     u'solar_elevation_angle': 44.5586114,
                     u'sw_version': u'LPGS_2.6.3',
                     u'terrain_correction': u'L1T',
                     u'tile_id': u'033034'},
     u'type': u'Feature'}


.. code:: ipython2

    # Let's plot the footprints of the scenes:
    lonlat_crs = cartopy.crs.PlateCarree()
    albers = cartopy.crs.AlbersEqualArea(central_latitude=36.0, central_longitude=-105)
    
    fig = plt.figure(figsize=(6, 8))
    ax = plt.subplot(projection=albers) # Specify projection of the map here
    
    ax.add_geometries([shapely.geometry.shape(shape['geometry'])],
                       lonlat_crs)
    
    # Get the geometry from each feature
    shapes = [shapely.geometry.shape(f['geometry']) for
              f in feature_collection['features']]
    
    ax.add_geometries(shapes, lonlat_crs, alpha=0.3, color='green')
    
    # Get a bounding box of the combined scenes
    union = shapely.geometry.MultiPolygon(polygons=shapes)
    bbox = union.bounds
    ax.set_extent((bbox[0], bbox[2], bbox[1], bbox[3]), crs=lonlat_crs)
    ax.gridlines(crs=lonlat_crs)
    
    plt.show()



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/getting_started_files/getting_started_12_0.png


What Bands are Available?
-------------------------

Now that you've found some imagery you might be interested in, let's
look at which bands are available.

There are two ways to query this information:

-  ``dl.raster.get_bands_by_constellation()``
-  ``dl.raster.get_bands_by_key()``

This will include all available bands for this constellation, including
both native bands such as "red", "green", "blue", as well as derived
bands like "ndvi".

.. code:: ipython2

    band_information = dl.raster.get_bands_by_constellation("landsat:LC08:PRE:TOAR")
    # or
    #band_information = dl.raster.get_bands_by_key(feature_collection['features'][0]['id'])
    pprint(band_information.keys())


.. parsed-literal::

    [u'thermal',
     u'ndvi',
     u'cloud',
     u'blue',
     u'qa_water',
     u'visual_cloud_mask',
     u'ndwi2',
     u'ndwi1',
     u'qa_snow',
     u'qa_cirrus',
     u'aerosol',
     u'red',
     u'rsqrt',
     u'nir',
     u'alpha',
     u'ndwi',
     u'evi',
     u'swir1',
     u'swir2',
     u'bright',
     u'green',
     u'qa_cloud',
     u'cirrus',
     u'bai']


.. code:: ipython2

    # There is metadata associated with each band. In this case, we can tell that the "red" band 
    # is stored as a UInt16 dataset, has a valid range of [0, 10000] which maps to [0, 1.0] in 
    # Top-of-atmosphere-reflectance.
    pprint(band_information['red'])


.. parsed-literal::

    {u'color': u'Red',
     u'dtype': u'UInt16',
     u'name': u'red',
     u'nbits': 14,
     u'nodata': None,
     u'physical_range': [0.0, 1.0],
     u'shortname': u'r',
     u'valid_range': [0, 10000]}


Rasterizing imagery
-------------------

There are two ways to rasterize an image:

-  ``dl.raster.raster()`` : Creates a geo-referenced file and returns it
   in the response
-  ``dl.raster.ndarray()`` : Returns a ``numpy`` ndarray object

Both of these methods can take either a single scene key or multiple
scene keys. If multiple are supplied, sources will be mosaic'd together
in order, so the first source will be on the bottom and the last on top.
We'll use the ndarray method in this tutorial.

See
http://descartes-labs-python.readthedocs.io/en/latest/api.html#raster
for more details.

.. code:: ipython2

    # Collect the id's for each feature
    ids = [f['id'] for f in feature_collection['features']]
    # Rasterize the features.
    #  * Select red, green, blue, alpha
    #  * Scale the incoming data with range [0, 10000] down to [0, 4000] (40% TOAR)
    #  * Choose an output type of "Byte" (uint8)
    #  * Choose 60m resolution
    #  * Apply a cutline of Taos county
    arr, meta = dl.raster.ndarray(
        ids,
        bands=['red', 'green', 'blue', 'alpha'],
        scales=[[0,4000], [0, 4000], [0, 4000], None],
        data_type='Byte',
        resolution=60,
        cutline=shape['geometry'],
    )
    
    # Note: A value of 1 in the alpha channel signifies where there is valid data. 
    # We use this throughout the majority of our imagery as a standard way of specifying
    # valid or nodata regions. This is particularly helpful if a value of 0 in a particular
    # band has meaning, rather than specifying a lack of data.

.. code:: ipython2

    # We'll use matplotlib to make a quick plot of the image.
    import matplotlib.pyplot as plt
    %matplotlib inline

.. code:: ipython2

    plt.figure(figsize=[16,16])
    plt.imshow(arr)




.. parsed-literal::

    <matplotlib.image.AxesImage at 0x7f7ca256d050>




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/getting_started_files/getting_started_19_1.png


.. code:: ipython2

    # We can choose other false color band combinations, like
    # NIR - SWIR1 - SWIR2
    arr, meta = dl.raster.ndarray(
        ids,
        bands=['nir', 'swir1', 'swir2', 'alpha'],
        scales=[[0,4000], [0, 4000], [0, 4000], None],
        cutline=shape['geometry'],
        data_type='Byte',
        resolution=60
    )
    plt.figure(figsize=[16,16])
    plt.imshow(arr)




.. parsed-literal::

    <matplotlib.image.AxesImage at 0x7f7ca1f21850>




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/getting_started_files/getting_started_20_1.png


.. code:: ipython2

    # Or even derived bands like NDVI. Here the alpha channel comes in
    # particularly useful as a way to select valid data. Here you may want to use
    # some of the band information to scale NDVI properly. 
    
    valid_range = band_information['ndvi']['valid_range']
    physical_range = band_information['ndvi']['physical_range']
    print "%s maps to %s" % (valid_range, physical_range)
    arr, meta = dl.raster.ndarray(
        [f['id'] for f in feature_collection['features']],
        bands=['ndvi', 'alpha'],
        scales=[[valid_range[0], valid_range[1], physical_range[0], physical_range[1]], None],
        cutline=shape['geometry'],
        data_type='Float32',
        resolution=60
    )


.. parsed-literal::

    [0, 65535] maps to [-1.0, 1.0]


.. code:: ipython2

    # Here we can make a numpy masked array using alpha == 0 as a nodata mask.
    import numpy as np
    mask = arr[:, :, 1] == 0
    masked_ndvi = np.ma.masked_array(arr[:, :, 0], mask)
    plt.figure(figsize=[16,16])
    plt.imshow(masked_ndvi, cmap='BrBG', vmin=0, vmax=0.5)
    cb = plt.colorbar()
    cb.set_label("NDVI")



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/getting_started_files/getting_started_22_0.png

