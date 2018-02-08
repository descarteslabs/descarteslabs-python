
DLTiles
-------

When working with satellite imagery it can be challening to apply an
analysis over a large area. In order to make this easier to do we have
created DLTiles in our platform. For a region defined by geojson or
latitude and longitude the platform will derive a list of tiles. These
tiles will be the same across various imagery sources allowing you to
easily work with data from different satellites. The tiles are projected
in UTM and support whatever resolution you request.

For a very simple example on a small area I am going to calculate the
land area of Rhode Island. I have written very simplistic method that
mask the pixels determined to be water and that then sum up all the
unmasked pixels giving us a total number of pixels that are land.

.. code:: ipython3

    import os
    from pprint import pprint
    import descarteslabs as dl
    dl.raster.url = 'https://platform-services.descarteslabs.com/raster/dev'
    import sys
    sys.path.append('/Users/conor/anaconda3/envs/descartes/Lib/site-packages') # yay windows
    %matplotlib inline
    import matplotlib.pyplot as plt
    import shapely.geometry
    import cartopy
    import json
    import numpy as np

    def mask_water(image):
        shape = image.shape
        length = image.size

        # reshape to linear
        x = image.reshape(length)

        # slice every 4th element
        y = x[0::4]

        # mask if less than 60 for NIR
        sixty = np.ones(len(y))*60
        z = y < sixty

        # multiply by 4
        a = np.repeat(z, 4)

        # apply mask to original array
        b = np.ma.masked_array(x, a)
        b = np.ma.filled(b, 0)

        # reshape
        c = b.reshape(shape)
        return c

    # returns a count of all the pixels in an image that haven't been masked out
    def get_land_pixel_count(image):
        length = image.size
        x = image.reshape(length)
        y = x[3::4]
        return np.count_nonzero(y)

    # get the geometry for Newport County, Rhode Island
    matches = dl.places.find('rhode-island_newport')
    aoi = matches[0]
    # get the shape of Newport County
    shape = dl.places.shape(aoi['slug'], geom='low')
    # get ids for imagery
    feature_collection = dl.metadata.search(products=['landsat:LC08:PRE:TOAR'], start_time='2016-06-01',
                                            end_time='2016-06-30', limit=10, place=aoi['slug'])
    ids = [f['id'] for f in feature_collection['features']]

    #rasterize the imagery and cut it to the shape of Newport County
    arr, meta = dl.raster.ndarray(
        ids,
        bands=['nir', 'swir1', 'red', 'alpha'],
        scales=[[0,6000], [0, 6000], [0, 6000], None],
        data_type='Byte',
        resolution=30,
        cutline = shape['geometry']
    )

    # elimiate water
    arr = mask_water(arr)

    # get land area
    print("land area = " + str(get_land_pixel_count(arr)*900) + " meters squared")

    # plot the pretty picture
    import matplotlib.pyplot as plt
    %matplotlib inline
    plt.figure(figsize=[24,24])
    plt.imshow(arr)




.. parsed-literal::

    land area = 266893200 meters squared




.. parsed-literal::

    <matplotlib.image.AxesImage at 0x180361ff780>




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_1_2.png


I have first run this analysis over Newport County, as shown above and
come up with land area of 266893200 or about 103 square miles. Newport
County has land area of 102 square miles which means our answer is
almost respectable. Lets see how we do for the whole state.

The first step is to get a set of DL Tiles for Rhode Island.

.. code:: ipython3

    lil_rhody = dl.places.shape("north-america_united-states_rhode-island")
    tiles = dl.raster.dltiles_from_shape(30.0, 2048, 16, lil_rhody)
    pprint(tiles['features'][0])
    pprint("Total number of tiles for Rhode Island: " + str(len(tiles['features'])))


.. parsed-literal::

    {'geometry': {'coordinates': [[[-71.92898332230831, 41.02873098011615],
                                   [-71.18735024674605, 41.04520331997488],
                                   [-71.20622433237934, 41.606871549447824],
                                   [-71.95423703966668, 41.590072611375206],
                                   [-71.92898332230831, 41.02873098011615]]],
                  'type': 'Polygon'},
     'properties': {'cs_code': 'EPSG:32619',
                    'key': '2048:16:30.0:19:-4:74',
                    'outputBounds': [253760.0, 4546080.0, 316160.0, 4608480.0],
                    'pad': 16,
                    'resolution': 30.0,
                    'ti': -4,
                    'tilesize': 2048,
                    'tj': 74,
                    'zone': 19},
     'type': 'Feature'}
    'Total number of tiles for Rhode Island: 4'


We have gotten 4 tiles of with a resolution of 30 meters, a size of 2048
pixels per side, and with an overlap between tiles of 16 pixels. We can
use any of the shapes from the places endpoint, a geojson, or use
latitude and longitude to define an area to be tiled. That area is then
divided up as appropriate and returned as a set. Lets take a look at how
our tiles relate to the shape of the state.

.. code:: ipython3



    lonlat_crs = cartopy.crs.PlateCarree()
    albers = cartopy.crs.AlbersEqualArea(central_latitude=41.0, central_longitude=-71)

    fig = plt.figure(figsize=(6, 8))
    ax = plt.subplot(projection=albers) # Specify projection of the map here

    ax.add_geometries([shapely.geometry.shape(lil_rhody['geometry'])],
                       lonlat_crs)

    # Get the geometry from each feature
    shapes = [shapely.geometry.shape(f['geometry']) for
            f in tiles['features']]
    ax.add_geometries(shapes, lonlat_crs, alpha=0.3, color='green')

    # Get a bounding box of the combined scenes
    union = shapely.geometry.MultiPolygon(polygons=shapes)
    bbox = union.bounds
    ax.set_extent((bbox[0], bbox[2], bbox[1], bbox[3]), crs=lonlat_crs)
    ax.gridlines(crs=lonlat_crs)

    plt.show()


.. parsed-literal::

    C:\Users\conor\Anaconda3\lib\site-packages\matplotlib\ticker.py:1693: UserWarning: Steps argument should be a sequence of numbers
    increasing from 1 to 10, inclusive. Behavior with
    values outside this range is undefined, and will
    raise a ValueError in future versions of mpl.
      warnings.warn('Steps argument should be a sequence of numbers\n'



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_5_1.png


Lets look at imagery for these tiles for July 2016. By using the data
contained in the tile for our raster call we're able to get the imagery
that corresponds with the tile. We also need to use a cutline that we
generated from the shapes endpoint to limit the imagery returned to just
the area of Rhode Island.

.. code:: ipython3

    dates = [['2016-07-01','2016-07-31']]

    tile_images = []

    for date in dates:
        print('from ' + date[0] + ' to ' + date[1])
        counter = 0;
        for tile in tiles['features']:
            images = dl.metadata.search(
                                    products=["landsat:LC08:PRE:TOAR"],
                                    start_time=date[0],
                                    end_time=date[1],
                                    geom=json.dumps(tile['geometry']),
                                    cloud_fraction=0.2,
                                    limit = 1000
                                    )

            print('number of scenes for this tile: ' + str(len(images['features'])))
            ids = []
            for image in images['features']:
                ids.append(image['id'])

            arr, meta = dl.raster.ndarray(
                ids,
                bands=['nir', 'swir1', 'red', 'alpha'],
                scales=[[0,6000], [0, 6000], [0, 6000], None],
                data_type='Byte',
                srs = tile['properties']['cs_code'],
                resolution = tile['properties']['resolution'],
                bounds = tile['properties']['outputBounds'],
                cutline = lil_rhody['geometry'])

            arr = arr[16:-16, 16:-16]

            tile_images.append([np.copy(arr),meta])

            plt.figure(figsize=[16,16])
            plt.imshow(arr)


.. parsed-literal::

    from 2016-07-01 to 2016-07-31
    number of scenes for this tile: 4
    number of scenes for this tile: 5
    number of scenes for this tile: 4
    number of scenes for this tile: 2



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_7_1.png



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_7_2.png



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_7_3.png



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_7_4.png


And look at that, Rhode Island all broken up into tiles ready to be
analyzed. So lets see how much land area we come up with for the whole
state.

.. code:: ipython3

    print('running land area analysis')

    total_land_pixels = 0

    for the_image in tile_images:
        meta = the_image[1]
        image_pixels = the_image[0]
        image_pixels = mask_water(image_pixels)
        plt.figure(figsize=[16,16])
        plt.imshow(image_pixels)
        cur_land_count = get_land_pixel_count(image_pixels)
        total_land_pixels += cur_land_count

    print("land area = " + str(total_land_pixels*900) + " meters squared")


.. parsed-literal::

    running land area analysis
    land area = 2658275100 meters squared



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_9_1.png



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_9_2.png



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_9_3.png



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/DLTiles_files/DLTiles_9_4.png


This gives ups 2658275100 square meters which works out to 1044 square
miles which is only 86% of the land area of Rhode Island. Judging by the
swiss cheese looking images of the state it is a safe guess that cloud
shadows are getting classified as water which accounts for the error.

Rhode Isand is a tiny little state that barely merits using tiles - lets
take a look at New York. Because New York is so much larger we'll go
with 60 meter resolution instead if 30.

.. code:: ipython3

    new_york = dl.places.shape("north-america_united-states_new-york")
    tiles = dl.raster.dltiles_from_shape(60.0, 2048, 16, new_york)
    pprint(tiles['features'][0])
    pprint("Total number of tiles for New York: " + str(len(tiles['features'])))


.. parsed-literal::

    {'geometry': {'coordinates': [[[-81.01142542548452, 41.061641925635584],
                                   [-79.52635307127217, 41.0522195452879],
                                   [-79.50054542369706, 42.17594834200205],
                                   [-81.01162560699078, 42.185747884904536],
                                   [-81.01142542548452, 41.061641925635584]]],
                  'type': 'Polygon'},
     'properties': {'cs_code': 'EPSG:32617',
                    'key': '2048:16:60.0:17:0:37',
                    'outputBounds': [499040.0, 4545600.0, 623840.0, 4670400.0],
                    'pad': 16,
                    'resolution': 60.0,
                    'ti': 0,
                    'tilesize': 2048,
                    'tj': 37,
                    'zone': 17},
     'type': 'Feature'}
    'Total number of tiles for New York: 32'


32 tiles, now we're talking! Lets see how much land area New York has.
**This will take a non-trivial amount of time to run.**

.. code:: ipython3

    dates = [['2016-06-01','2016-06-30']]

    total_land_pixels = 0
    counter = 1

    for date in dates:
        print('from ' + date[0] + ' to ' + date[1])
        counter = 0;
        for tile in tiles['features']:
            images = dl.metadata.search(
                                    products=["landsat:LC08:PRE:TOAR"],
                                    start_time=date[0],
                                    end_time=date[1],
                                    geom=json.dumps(tile['geometry']),
                                    cloud_fraction=0.2,
                                    limit = 1000
                                    )

            print('Tile #' + str(counter) + '. Number of scenes for this tile: ' + str(len(images['features'])))
            counter += 1
            ids = []
            for image in images['features']:
                ids.append(image['id'])

            arr, meta = dl.raster.ndarray(
                ids,
                bands=['nir', 'swir1', 'red', 'alpha'],
                scales=[[0,6000], [0, 6000], [0, 6000], None],
                data_type='Byte',
                srs = tile['properties']['cs_code'],
                resolution = tile['properties']['resolution'],
                bounds = tile['properties']['outputBounds'],
                cutline = new_york['geometry'])

            arr = arr[16:-16, 16:-16]

            arr = mask_water(arr)
            total_land_pixels += get_land_pixel_count(arr)

    print('total land pixels: ' + str(total_land_pixels))
    print('square meters: ' + str(total_land_pixels * 3600))


.. parsed-literal::

    from 2016-06-01 to 2016-06-30
    Tile #0. Number of scenes for this tile: 6
    Tile #1. Number of scenes for this tile: 6
    Tile #2. Number of scenes for this tile: 4
    Tile #3. Number of scenes for this tile: 4
    Tile #4. Number of scenes for this tile: 5
    Tile #5. Number of scenes for this tile: 2
    Tile #6. Number of scenes for this tile: 2
    Tile #7. Number of scenes for this tile: 3
    Tile #8. Number of scenes for this tile: 4
    Tile #9. Number of scenes for this tile: 2
    Tile #10. Number of scenes for this tile: 2
    Tile #11. Number of scenes for this tile: 3
    Tile #12. Number of scenes for this tile: 4
    Tile #13. Number of scenes for this tile: 3
    Tile #14. Number of scenes for this tile: 3
    Tile #15. Number of scenes for this tile: 5
    Tile #16. Number of scenes for this tile: 6
    Tile #17. Number of scenes for this tile: 6
    Tile #18. Number of scenes for this tile: 5
    Tile #19. Number of scenes for this tile: 4
    Tile #20. Number of scenes for this tile: 5
    Tile #21. Number of scenes for this tile: 7
    Tile #22. Number of scenes for this tile: 6
    Tile #23. Number of scenes for this tile: 7
    Tile #24. Number of scenes for this tile: 5
    Tile #25. Number of scenes for this tile: 5
    Tile #26. Number of scenes for this tile: 4
    Tile #27. Number of scenes for this tile: 5
    Tile #28. Number of scenes for this tile: 3
    Tile #29. Number of scenes for this tile: 5
    Tile #30. Number of scenes for this tile: 3
    Tile #31. Number of scenes for this tile: 5
    total land pixels: 30787583
    square meters: 110835298800


This gives us 44,449 square miles which is 81 percent of the 54,556
square miles that actually make up New York state. Ultimately I wrote a
pretty terrible algorithm for analysis but using DLTiles it was very
easy for us to determine that, and it would also be easy to iterate on
this and turn it into a good algorithm. Using DLTiles we can scale our
analysis up all the way to the entire surface of the Earth.