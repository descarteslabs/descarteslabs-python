
Intro to Land Cover with the Descartes Labs Platform
----------------------------------------------------

In our tech blog, we introduced an application of the Descartes Lab
Platform where we mapped croplands in Brazil. We thought we would get a
little more technical and demonstrate how to implement some of those
steps with Platform APIs.

We'll start by looking at one cropland and non-cropland training point
each located in Mato Grosso, Brazil, and pull the relevant MODIS imagery
containing the point. Then we'll calculate annual NDVI statistics - the
same ones used for training the classifier in our cropland
classification. Once we calculate statistics over the image, we'll
extract data for each of the training points and format it for
classification.

.. code:: ipython2

    from __future__ import print_function
    
    import descarteslabs as dl
    
    %matplotlib inline
    import matplotlib.pyplot as plt
    import shapely.geometry
    import cartopy
    import numpy as np
    import pyproj

Define the training point
~~~~~~~~~~~~~~~~~~~~~~~~~

To run a full-scale classification, we would normally read in a GeoJSON
file containing hundreds or even thousands of reference points. Here,
we'll stick to two points to walk through the data extraction process on
the Platform. We've pulled these reference sites from the GFSAD30
reference sample and distilled the attribute data down to a few pieces
of information relevant for our task.

.. code:: ipython2

    reference_points = { "type": "FeatureCollection",
                        
                         "crs": { "type": "name", 
                                  "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84"} },
                        
                         "features": [
        
                            { "type": "Feature", 
                               "id": 14348, 
                               "properties": { 
                                    "descript": "Cultivated Crops",  
                                    "created": "2016-3", 
                                    "lc_code": 1, 
                                    "dataset": "GFSAD30" }, 
                              "geometry": { "type": "Point", 
                                           "coordinates": [ -57.864114, -13.458213 ] }}, 
                            
                        { "type": "Feature", 
                               "id": 30895, 
                               "properties": { 
                                    "descript": "Forest",  
                                    "created": "2016-0", 
                                    "lc_code": 2, 
                                    "dataset": "GFSAD30" }, 
                              "geometry": { "type": "Point", 
                                           "coordinates": [ -57.345734, -12.748814 ] }}
                        
                                    ]
                       }

Define the study region
~~~~~~~~~~~~~~~~~~~~~~~

Let's look at imagery over an agricultural region in the state of Mato
Grosso. We'll first load the geometry of the state, then choose a small
subset containing both points to pull imagery from.

.. code:: ipython2

    # Get attribute data for the point
    point = reference_points['features'][0]
    lon = point['geometry']['coordinates'][0]
    lat = point['geometry']['coordinates'][1]
    year = point['properties']['created'][:4]
    
    # calculate the UTM zone using longitude
    zone = int((lon + 180) / 6.0) + 1
    
    # construct a tile around the centroid 
    # with 833x833 valid image pixels
    # with 16 pixels of padding around each grid cell
    # and grid spatial resolution of 120m
    valid_pix = 833
    padding = 0
    grid_size = valid_pix + padding
    res = 120.0
    dltile = dl.raster.dltile_from_latlon(lat, lon, res, valid_pix, padding)
    
    
    # Let's visualize the state and the dltile.
    
    # load the lat/lon and utm projections
    lonlat_crs = cartopy.crs.PlateCarree()
    utm = cartopy.crs.UTM(zone, southern_hemisphere=True)
    
    # Let's get the geometry for Mato Grosso for visualization
    mato_grosso = dl.places.shape('south-america_brazil_mato-grosso')
    
    # Plot Mato Grosso and the image tile containing the reference point
    fig = plt.figure(figsize=(6, 6))
    ax = plt.subplot(projection=utm) # Specify projection of the map here
    shp = shapely.geometry.shape(mato_grosso['geometry'])
    shp2 = shapely.geometry.shape(dltile['geometry'])
    
    # Add geometry with lat/long, specifying the latlon projection
    ax.add_geometries([shp], lonlat_crs, color='#bece96')
    ax.add_geometries([shp2], lonlat_crs, color='#2e89f7', edgecolor='k', hatch='//')
    
    # Set extents in latlon, specifying the latlon projection
    bbox = mato_grosso['bbox']
    ax.set_extent((bbox[0], bbox[2], bbox[1], bbox[3]), crs=lonlat_crs)
    ax.gridlines(crs=lonlat_crs)
    plt.show()



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/land_cover_demo_files/land_cover_demo_5_0.png


Request imagery
~~~~~~~~~~~~~~~

Now we'll search for imagery covering our area of interest. We'll pull
any data from MODIS acquired over 2016 as long as the scene is less than
90% cloud covered.

.. code:: ipython2

    # use const_id, start_time, end_time, geom, and cloud_fraction
    # parameters to limit our imagery search to 
    
    images = dl.metadata.search(
                                    const_id=["MO", "MY"],
                                    start_time='2016-01-01',
                                    end_time='2016-12-31',
                                    geom=dltile['geometry'],
                                    cloud_fraction=0.9,
                                    limit = 1000
                                    )
    
    
    n_images = len(images['features'])
    print('Number of image matches: %d' % n_images)


.. parsed-literal::

    Number of image matches: 468


.. code:: ipython2

    # let's see which bands are available from both sensors
    
    mo = dl.raster.get_bands_by_constellation("MO").keys()
    my = dl.raster.get_bands_by_constellation("MY").keys()
    avail_bands = set(mo).intersection(my)
    print('Available bands: %s' % ', '.join([a for a in avail_bands]))


.. parsed-literal::

    Available bands: blue, evi, z_sensor, visual_cloud_mask, rsqrt, AOD1, cloudfree, z_sun, phi_delta, ndvi, red, green, water_vapor, AOD2, AOD3, nir, alpha, ndvi_derived, cirrus, path_radiance, bai


Calculate NDVI statistics
~~~~~~~~~~~~~~~~~~~~~~~~~

NDVI is available from both sensors, so we'll calculate annual
statistics of NDVI for 2016 over each pixel. We'll use the derived
version that we calculate from Red and NIR on the fly.

.. code:: ipython2

    # create an empty list to store numpy arrays
    arr_list = []
    
    # get band information for NDVI from MODIS - we'll use this for scaling the data
    band_info = dl.raster.get_bands_by_constellation("MO")
    valid_range = band_info['ndvi_derived']['valid_range']
    physical_range = band_info['ndvi_derived']['physical_range']
    
    for feature in images['features']:
        
        # get the scene id
        scene = feature['id']
    
    
        # load the image data into a numpy array
        try:
            arr, meta = dl.raster.ndarray(
            scene,
            resolution=dltile['properties']['resolution'],
            bounds=dltile['properties']['outputBounds'],
            srs=dltile['properties']['cs_code'], 
            bands=['ndvi_derived', 'alpha'],
            scales=[[valid_range[0], valid_range[1], physical_range[0], physical_range[1]], None],
            data_type='Float32'
            )
        except:
            print('%s could not be retreived' % scene)
            continue
        
        # mask out nodata pixels
        nodata = arr[:,:,-1] == 0
        masked = np.where(nodata, 0, arr[:,:,0])
    
        # add masked ndvi array to list
        arr_list.append(masked)
        
        del arr
        del masked
    
    # convert list of arrays to numpy array    
    ndvi = np.asarray(arr_list)
    
    # calculate statistics over time dimension
    max_ndvi = np.ma.masked_equal(ndvi, 0).max(axis=0)
    min_ndvi = np.ma.masked_equal(ndvi, 0).min(axis=0)
    mean_ndvi = np.ma.masked_equal(ndvi, 0).mean(axis=0)
    std_ndvi = np.ma.masked_equal(ndvi, 0).std(axis=0)

Let's take a look at the mean NDVI over the year for the region and
visualize it using matplotlib.

.. code:: ipython2

    fig, ax = plt.subplots(figsize=[12,12])
    cax = ax.imshow(mean_ndvi, "BrBG")
    ax.set_title('Mean NDVI over 2016')
    cbar = fig.colorbar(cax, orientation='vertical')
    cbar.set_ticks([mean_ndvi.min(),mean_ndvi.max()])
    cbar.set_ticklabels([physical_range[0], physical_range[1]])
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    plt.show()



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/land_cover_demo_files/land_cover_demo_12_0.png


Croplands here are visibly identifiable as brown, regularly shaped
fields. These croplands are surrounded by tropical forests, which have a
much higher average NDVI. Why? To start, the vegetation content of
agricultural fields may vary tremendously throughout the year - from
peak growth to post-harvest, when there may be no vegetation at all left
on the field. Conversely, evergreen forests maintain their leafy matter
all year long. Vegetative signal also varies from crop to crop depending
on the plant structure and physiology, so even at peak greenness a crop
may be (and usually is) significantly less green than a forest. If these
croplands were instead surrounded by developed areas or bare ground,
they would look much greener (higher NDVI) relative to their
surroundings.

Extract data for cropland reference point
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now we'll extract data for the two training points from each of the NDVI
composites. We'll first use the affine transform to convert the
geographic coordinates to pixel coordinates based on the raster image
size, resolution, and rotation. Then, we can simply reference the array
indices to access the NDVI values. We'll save the data and labels in two
separate lists, which is the format used for the sklearn random forest
classifiers we used in the cropland classification.

.. code:: ipython2

    # get projection information from imagery
    prj = dltile['properties']['cs_code']
    minx, miny, maxx, maxy = dltile['properties']['outputBounds']
    
    # convert point from WGS (lat/lon) to UTM
    wgs84=pyproj.Proj("+init=EPSG:4326")
    utm = pyproj.Proj("+init=%s" % prj)
    
    # lists to store feature data and labels
    features = []
    labels = []
    
    print('{:>20} {:>15} {:>10} {:>10} {:>10} {:>10}\n'.format('Label', 'Coordinates', 'Max', 'Min', 'Mean', 'Std'))
    for point in reference_points['features']:
        
        lon = point['geometry']['coordinates'][0]
        lat = point['geometry']['coordinates'][1]
        x,y = pyproj.transform(wgs84,utm,lon,lat)
        
        # affine transformation to pixel coordiantes
        pix_y = (y - maxy + (minx - x) / res * 0) / (-res - 0 / res * 0)
        pix_x = (x - minx - pix_y * 0) / res 
        
        # round down to pixel x,y
        pix_y = int(round (pix_y))
        pix_x = int(round (pix_x))
    
        # get NDVI statistics for pixel
        
        feature = []
        for stat in [max_ndvi, min_ndvi, mean_ndvi, std_ndvi]:
            feature.append(stat[pix_y-1, pix_x-1])
    
        # append to feature and labels lists
        features.append(feature)
        labels.append(point['properties']['lc_code'])
        
        print('{:>20} {:>15} {:>10.2f} {:>10.2f} {:>10.2f} {:>10.2f}'.format(point['properties']['descript'], (pix_y, pix_x), feature[0], feature[1], feature[2], feature[3]))


.. parsed-literal::

                   Label     Coordinates        Max        Min       Mean        Std
    
        Cultivated Crops       (738, 54)       0.91      -1.00       0.40       0.39
                  Forest       (83, 520)       1.00      -1.00       0.72       0.29


We can see that the cropland pixel has many of the spectral
characteristics we expect - a high max NDVI, but lower than the
neighboring forest, and a low mean NDVI but a higher standard deviation
relative to forest, both related to change in cropland vegetation over
the year.
