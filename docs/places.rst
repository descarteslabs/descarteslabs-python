
Places
======

Named shapes and associated statistics

.. code:: ipython3

    import os
    # Instead of using the `descarteslabs login` command, you can manually set the token information
    # os.environ['CLIENT_ID'] = '<YOUR ID HERE>'
    # os.environ['CLIENT_SECRET'] = '<YOUR SECRET HERE>'

.. code:: ipython3

    %matplotlib inline
    import matplotlib.pyplot as plt
    import shapely
    import cartopy
    import cartopy.mpl.gridliner
    
    import descarteslabs as dl

Test connectivity by getting a list of known placetypes:

.. code:: ipython3

    dl.places.placetypes()




.. parsed-literal::

    ['country', 'region', 'district', 'mesoregion', 'microregion', 'county']



The "find" operation searches using exact string matching but with more
flexible ordering of the slug:

.. code:: ipython3

    dl.places.find('iowa')




.. parsed-literal::

    [{'bbox': [-96.639468, 40.37544, -90.140061, 43.501128],
      'id': 85688713,
      'name': 'Iowa',
      'path': 'continent:north-america_country:united-states_region:iowa',
      'placetype': 'region',
      'slug': 'north-america_united-states_iowa'}]



For example, maybe you don't remember what district Cass County, Iowa is
in. Order doesn't matter.

.. code:: ipython3

    dl.places.find('cass_iowa')




.. parsed-literal::

    [{'bbox': [-95.155851, 41.158246, -94.700434, 41.505211],
      'id': 102086611,
      'name': 'Cass',
      'path': 'continent:north-america_country:united-states_region:iowa_district:southwest_county:cass',
      'placetype': 'county',
      'slug': 'north-america_united-states_iowa_southwest_cass'}]



.. code:: ipython3

    dl.places.find('iowa_cass')




.. parsed-literal::

    [{'bbox': [-95.155851, 41.158246, -94.700434, 41.505211],
      'id': 102086611,
      'name': 'Cass',
      'path': 'continent:north-america_country:united-states_region:iowa_district:southwest_county:cass',
      'placetype': 'county',
      'slug': 'north-america_united-states_iowa_southwest_cass'}]



If all you need is the bounding box for the shape, you are done. You can
also request the exact shape using the full slug.

.. code:: ipython3

    shape = dl.places.shape('north-america_united-states_iowa', geom='low')
    bbox = shape['bbox']
    
    # Lets load up the Albers Equal Area projection.
    lonlat_crs = cartopy.crs.PlateCarree()
    albers = cartopy.crs.AlbersEqualArea(central_latitude=40.0, central_longitude=-95)
    
    fig = plt.figure(figsize=(10, 14))
    ax = plt.subplot(projection=albers) # Specify projection of the map here
    shp = shapely.geometry.shape(shape['geometry'])
    
    # When adding a geometry in latlon coordinates, specify the latlon projection
    ax.add_geometries([shp], lonlat_crs)
    
    # You can set extents in latlon, as long as you specify the projection with `crs`
    ax.set_extent((bbox[0], bbox[2], bbox[1], bbox[3]), crs=lonlat_crs)
    ax.gridlines(crs=lonlat_crs)
    plt.show()



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/places_files/places_11_0.png


``shape()`` returns "low" resolution geometry by default (which is still
pretty good). There are also medium and high resolution shapes if you
prefer.

.. code:: ipython3

    shape = dl.places.shape('north-america_united-states_iowa', geom='high')

Another common operation is to get all the shapes that are "beneath"
another in the hierarchy using a slug prefix:

.. code:: ipython3

    results = dl.places.prefix('north-america_united-states_iowa', placetype='county')
    
    bbox = results['bbox']
    county_shapes = [shapely.geometry.shape(shape['geometry']) for shape in results['features']]
    
    lonlat_crs = cartopy.crs.PlateCarree()
    albers = cartopy.crs.AlbersEqualArea(central_latitude=40.0, central_longitude=-95)
    
    # Specify projection of the map here
    fig = plt.figure(figsize=(10, 14))
    ax = plt.subplot(projection=albers)
    
    ax.add_geometries(county_shapes, lonlat_crs, linewidth=0.5, edgecolor='black')
    ax.set_extent((bbox[0], bbox[2], bbox[1], bbox[3]), crs=lonlat_crs)
    ax.gridlines(crs=lonlat_crs)
    plt.show()



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/places_files/places_15_0.png


``prefix()`` can also produce
`TopoJSONs <https://github.com/topojson/topojson>`__

.. code:: ipython3

    results = dl.places.prefix('north-america_united-states_iowa', placetype='county', output='topojson')
