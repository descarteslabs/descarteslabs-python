
Fighting Wildfires Using a Cloud-based Supercomputer
====================================================

Let's use the Descartes Labs platform to assess and quantify the degree
of damage a wildfire inflicts upon the land. This demo focuses on
California's most expensive fire in history, the 2016 Soberanes Fire on
the beautiful Monterey coast and Big Sur National Park.

.. code:: ipython2

    # Let's start with importing all the toolboxes we will need for both analysis and vizualization
    from IPython.display import display, Image
    from descarteslabs.services import Places
    from descarteslabs.services import Metadata
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    from copy import deepcopy
    from skimage import measure
    from skimage.morphology import dilation #, erosion, opening, closing, white_tophat
    from skimage.morphology import disk
    from pprint import pprint
    from pylab import *
    import descarteslabs as dl
    import numpy as np
    import matplotlib.pyplot as plt

    %matplotlib inline

Retreive imagery
----------------

.. code:: ipython2

    # First let's define the AOI as the county in which the Soberanes fire occurred

    # Find potential AOI matches
    matches = dl.places.find('california_monterey')
    pprint(matches)
    # The first one looks good, so let's make that our area of interest.
    aoi = matches[0]
    shape = dl.places.shape(aoi['slug'], geom='low')


.. parsed-literal::

    [{u'bbox': [-122.051878, 35.789276, -120.213979, 36.919683],
      u'id': 102080859,
      u'name': u'Monterey',
      u'path': u'continent:north-america_country:united-states_region:california_district:central-coast_county:monterey',
      u'placetype': u'county',
      u'slug': u'north-america_united-states_california_central-coast_monterey'}]


.. code:: ipython2

    # Check for imagery before the start date of July 22nd

    feature_collection = dl.metadata.search(const_id='L8', start_time='2016-07-22', end_time='2016-07-31',
                                            limit=10, place=aoi['slug'])
    # As the variable name implies, this returns a FeatureCollection GeoJSON dictionary.
    # Its 'features' are the available scenes.

    print(len(feature_collection['features']))
    # The 'id' associated with each feature is a unique identifier into our imagery database.
    # In this case there are 4 L8 scenes from adjoining WRS rows.
    print([f['id'] for f in feature_collection['features']])

    # Now check for imagery in late October, i.e., towards the end of the fire
    feature_collection = dl.metadata.search(const_id='L8', start_time='2016-10-15', end_time='2016-10-31',
                                            limit=10, place=aoi['slug'])

    print(len(feature_collection['features']))
    print([f['id'] for f in feature_collection['features']])

    # Let's print out all the available bands we have for Landsat 8
    L8_bands = dl.raster.get_bands_by_constellation("L8").keys()
    print(L8_bands)
    # Even though the 'bai' listed here stands for Burn Area Index, we need a normalized version of this index
    # We get the NBR (normalized burn ratio) by using the swir2 and nir bands


.. parsed-literal::

    4
    [u'meta_LC80430342016204_v1', u'meta_LC80430352016204_v1', u'meta_LC80440342016211_v1', u'meta_LC80440352016211_v1']
    5
    [u'meta_LC80440342016291_v1', u'meta_LC80440352016291_v1', u'meta_LC80420352016293_v1', u'meta_LC80430342016300_v1', u'meta_LC80430352016300_v1']
    [u'thermal', u'ndvi', u'cloud', u'blue', u'qa_water', u'visual_cloud_mask', u'ndwi2', u'ndwi1', u'qa_snow', u'qa_cirrus', u'aerosol', u'red', u'rsqrt', u'nir', u'alpha', u'ndwi', u'evi', u'swir1', u'swir2', u'bright', u'green', u'qa_cloud', u'cirrus', u'bai']


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
        scales=[[0,2048], [0, 2048], [0, 2048], None],
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
    plt.figure(figsize=[16,16])
    plt.axis('off')
    plt.imshow(arr)



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_6_0.png


.. code:: ipython2

    # Let's choose a different band combination to look at the fire scar
    # Rasterize the features.
    #  * Select swir2, nir, aerosol, alpha
    #  * Scale the incoming data with range [0, 10000] down to [0, 4000] (40% TOAR)
    #  * Choose an output type of "Byte" (uint8)
    #  * Choose 60m resolution for quicker vizualiation
    #  * Apply a cutline of Taos county
    arr, meta = dl.raster.ndarray(
        ids,
        bands=['swir2', 'nir', 'aerosol', 'alpha'],
        scales=[[0,4000], [0, 4000], [0, 4000], None],
        data_type='Byte',
        resolution=60,
        cutline=shape['geometry'],
    )
    # We'll use matplotlib to make a quick plot of the image.
    plt.figure(figsize=[16,16])
    plt.axis('off')
    plt.imshow(arr)




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_7_0.png


.. code:: ipython2

    # Now let's track activity in this AOI over 4 time windows and look at the 4 false color images

    times=[['2016-07-01','2016-07-20'], ['2016-07-22','2016-07-31'],
                 ['2016-08-01','2016-08-15'], ['2016-10-15','2016-10-31']]

    axes = [[0,0],[0,1],[1,0],[1,1]]
    fig, ax = plt.subplots(2,2,figsize=[20,15], dpi=300)
    ax=ax.flatten()
    for iax in ax.reshape(-1):
        iax.get_xaxis().set_ticks([])
        iax.get_yaxis().set_ticks([])

    for i, timewindow in enumerate(times):
        feature_collection = dl.metadata.search(const_id='L8', start_time=timewindow[0], end_time=timewindow[1],
                                            limit=10, place=aoi['slug'])
        ids = [f['id'] for f in feature_collection['features']]
        arr, meta = dl.raster.ndarray(
            ids,
            bands=['swir2', 'nir', 'aerosol', 'alpha'],
            scales=[[0,4000], [0, 4000], [0, 4000], None],
            data_type='Byte',
            resolution=60,
            cutline=shape['geometry'],
        )
        #ax[axes[i][0], axes[i][1]].imshow(arr)
        ax[i].imshow(arr)
        ax[i].set_xlabel('%s' %timewindow[1] , fontsize=24)

    fig.suptitle('Monterey County: Soberanes Fire Progress', size=24)
    fig.subplots_adjust(left=0.05, right=0.95, top=0.95, bottom=0.025, wspace=0.025, hspace=0.025)




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_8_0.png


Obtain the Delta Normalized Burn Ratio
--------------------------------------

.. code:: ipython2

    # The Normalized Burn Ratio (NBR) is defined as NBR = (nir-swir2)/(nir+swir2)
    # The NBR values will be in a [-1, 1] physical range and we need full bit depth to compute a good index
    # We will request the raw band rasters scaled now to 10000 (100% TOAR) to obtain a correct analytic value

    # Get pre-fire NBR
    feature_collection = dl.metadata.search(const_id='L8', start_time='2016-07-01', end_time='2016-07-20',
                                            limit=10, place=aoi['slug'])
    ids = [f['id'] for f in feature_collection['features']]
    arr, meta = dl.raster.ndarray(
        ids,
        bands=['swir2', 'nir','alpha'],
        scales=[[0,10000], [0, 10000], None],
        data_type='UInt16',
        resolution=30,
        cutline=shape['geometry'],
    )

    arr = arr.astype('double')
    prenbr = (arr[:,:,1]-arr[:,:,0])/(arr[:,:,1]+arr[:,:,0]+1e-9)

    # Get post-fire NBR
    feature_collection = dl.metadata.search(const_id='L8', start_time='2016-10-15', end_time='2016-10-31',
                                            limit=10, place=aoi['slug'])
    ids = [f['id'] for f in feature_collection['features']]
    arr, meta = dl.raster.ndarray(
        ids,
        bands=['swir2', 'nir','alpha'],
        scales=[[0,10000], [0, 10000], None],
        data_type='UInt16',
        resolution=30,
        cutline=shape['geometry'],
    )

    arr = arr.astype('double')
    postnbr = (arr[:,:,1]-arr[:,:,0])/(arr[:,:,1]+arr[:,:,0]+1e-9)

    deltanbr = prenbr - postnbr

    # Let's check the ranges of these indices
    print(prenbr.max(), prenbr.min(), type(prenbr))
    print(postnbr.max(), postnbr.min(), type(postnbr))
    print(deltanbr.max(), deltanbr.min(), type(deltanbr))

    fig, ax = plt.subplots(1,3,figsize=[30,10], dpi=300)
    ax=ax.flatten()
    for iax in ax.reshape(-1):
        iax.get_xaxis().set_ticks([])
        iax.get_yaxis().set_ticks([])

    # And let's zoom in to the Soberanes fire perimeter and look at the two NBR images side by side
    ax[0].imshow(prenbr[1525:2900, 350:1800],cmap='hsv') #cmap='nipy_spectral'
    ax[1].imshow(postnbr[1525:2900, 350:1800],cmap='hsv')
    ax[2].imshow(deltanbr[1525:2900, 350:1800],cmap='coolwarm')

    # Add some labels and markings
    ax[0].set_xlabel('Pre-fire NBR' , fontsize=24)
    ax[1].set_xlabel('Post-fire NBR' , fontsize=24)
    ax[2].set_xlabel('Delta NBR' , fontsize=24)

    fig.suptitle('Soberanes Fire Area: Normalized Burn Ratios', size=28)
    fig.subplots_adjust(top = 0.98, wspace=0.025, hspace=0.025)



.. parsed-literal::

    (0.99999999996296296, -0.99999999966666664, <type 'numpy.ndarray'>)
    (0.99999999997560984, -0.99999999979999998, <type 'numpy.ndarray'>)
    (1.3846153844005917, -1.3333333328888888, <type 'numpy.ndarray'>)



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_10_1.png


Computing the burn severity index map
-------------------------------------

.. code:: ipython2

    # We need to apply the thresholding proposed by the USGS FireMon program
    # < -0.25	High post-fire regrowth
    # -0.25 to -0.1	Low post-fire regrowth
    # -0.1 to +0.1	Unburned
    # 0.1 to 0.27	Low-severity burn
    # 0.27 to 0.44	Moderate-low severity burn
    # 0.44 to 0.66	Moderate-high severity burn
    # > 0.66	High-severity burn

    # First make a copy of the delta NBR array
    deltanbrcat = deepcopy(deltanbr)
    deltanbrcat[(deltanbr<0.1)]=0

    deltanbrcat[((deltanbr >=0.1) & (deltanbr <0.27))]=1
    deltanbrcat[(deltanbr >=0.27) & (deltanbr <0.44)]=2
    deltanbrcat[(deltanbr >=0.44) & (deltanbr <0.66)]=3
    deltanbrcat[(deltanbr >=0.66)]=4

    # zoom to Soberanes fire area only
    fire_dnbr = deltanbrcat[1525:2900, 350:1800]

    # Plot the severity index map we just derived
    ax = plt.figure(figsize=[16,16])
    plt.title('Soberanes Fire Burn Severity Index', fontsize=22)
    plt.tick_params(
        axis='both',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom='off',      # ticks along the bottom edge are off
        top='off',         # ticks along the top edge are off
        labelbottom='off', # labels along the bottom edge are off
        left='off',      # ticks along the bottom edge are off
        right='off',
        labelleft='off')         # ticks along the top edge are off
    ax = plt.imshow(fire_dnbr)

    # And let's add a colorbar labeled with our categories
    ax1 = plt.gca()
    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.4)
    cax.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom='off',      # ticks along the bottom edge are off
        top='off',         # ticks along the top edge are off
        labelbottom='off', # labels along the bottom edge are off
        left='off',      # ticks along the bottom edge are off
        right='off',
        labelleft='off')         # ticks along the top edge are off
    cbar = plt.colorbar(ax,cax=cax,ticks=[0, 1,2,3, 4])
    cbar.ax.set_yticklabels(['N/A', 'Low', 'Moderate-low', 'Moderate-high', 'High'],  fontsize=18)





.. parsed-literal::

    [<matplotlib.text.Text at 0x7f3913b75a10>,
     <matplotlib.text.Text at 0x7f3913b31390>,
     <matplotlib.text.Text at 0x7f3913af1550>,
     <matplotlib.text.Text at 0x7f3913af1c50>,
     <matplotlib.text.Text at 0x7f3913aff390>]




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_12_1.png


Derive the burn scar mask and the fire perimeter
------------------------------------------------

.. code:: ipython2

    # We mask out all pixels appearing unburned in the severity index map
    image = (fire_dnbr>1.0)*1.0

    # We use the skimage package to dilate this mask and make it smoother at the edges
    selem=disk(6)
    dilated = dilation(image, selem)

    # We find the countours on the dilated mask
    contours = measure.find_contours(dilated, 0.8)

    # Display the image and plot all contours found
    fig, ax = plt.subplots(figsize=(16,16))
    ax.imshow(image, interpolation='nearest', cmap=plt.cm.gray)
    maxlen=0
    for item in contours:
        if len(item)>maxlen:
            maxlen=len(item) #find which of the detected contours is the fire perimeter

    for n, contour in enumerate(contours):
        if len(contour)==maxlen:
            the_contour = contour
            ax.plot(contour[:, 1], contour[:, 0], linewidth=2)

    ax.axis('image')
    ax.set_xticks([])
    ax.set_yticks([])

    ax.set_title('Soberanes Burn Scar and Fire Contour', fontsize=24)





.. parsed-literal::

    <matplotlib.text.Text at 0x7f3913b22050>




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_14_1.png


Evaluate the distribution of burn severity within the fire perimeter
--------------------------------------------------------------------

.. code:: ipython2

    # Recopy the original DeltaNBR as the previous operations altered the values
    deltanbrcat = deepcopy(deltanbr)
    fire_dnbr = deltanbrcat[1525:2900, 350:1800]
    print(fire_dnbr.min(), fire_dnbr.max())

    # Get extent of a pixel grid for the fire scar
    nx, ny = (fire_dnbr.shape[0], fire_dnbr.shape[1])

    x = np.arange(-100, 100, 1)
    y = np.arange(0, 32000, 1)

    fire_mask = measure.grid_points_in_poly((nx, ny), the_contour )

    burned_pixels = fire_dnbr[fire_mask]*1000 #scaled by 1000 to get dNBR

    fig, ax = plt.subplots(figsize=(10,6))
    counts, bins, patches = ax.hist(burned_pixels, bins=80, facecolor='blue', edgecolor='gray')

    # Make matplotlib work for you and not against you
    ax.set_xlim([-500, 1400])
    ax.set_ylim([0, 32000])

    ax.set_ylabel('Severity frequency (pixels)', fontsize=12)
    ax.set_xlabel('dNBR', fontsize=12)

    ax.set_xticks(range(-500,1400,100))
    ax.set_xticklabels( range(-500,1400,100), rotation=45 )

    ax.axvline(x=-100,lw=1.33, ls='--')
    ax.axvline(x=100,lw=1.33, ls='--')

    ax.annotate('Unburned', xy=(0, 31000), xycoords='data',horizontalalignment='center', verticalalignment='center', fontsize=12)
    ax.annotate('Increasing burn severity', xy=(150, 31000), xycoords='data',horizontalalignment='left', verticalalignment='center', fontsize=12)
    ax.arrow(700, 31000, 250, 0, shape='full', lw=2, length_includes_head=True,head_width=500,head_length=50, fc='k', ec='k')
    ax.fill_between(x, 0, 32000, color='grey', alpha=0.5)

    # And now let's use an intuitive color scheme to highlight the different burn severity categories
    for patch, binm in zip(patches, bins):
        if (binm >= 660) :
            patch.set_facecolor('red')
        if (binm < 660) & (binm >= 440) :
            patch.set_facecolor('orange')
        if (binm < 440) & (binm >= 270) :
            patch.set_facecolor('yellow')
        if (binm < 270) & (binm >= 100) :
            patch.set_facecolor('green')
        if binm <100:
            patch.set_facecolor('blue')

    ax.set_title('Burn severity index histogram within fire contour', fontsize=14)



.. parsed-literal::

    (-1.0608407079561213, 1.2711069418195802)




.. parsed-literal::

    <matplotlib.text.Text at 0x7f39137bf250>




.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_16_2.png


Fire acreage stats
------------------

.. code:: ipython2

    # Acres reported by officials: 132,127 acres

    # Acreage of a square meter
    acrem = 0.000247105

    # Compute burn severity category respective acreage relative to total burned pixels
    total_within_contour = fire_mask.sum()*30*30*acrem
    total = (burned_pixels>=100).sum()*30*30*acrem
    low = ((burned_pixels >=100) & (burned_pixels <270)).sum()*30*30*acrem
    mlow = ((burned_pixels >=270) & (burned_pixels <440)).sum()*30*30*acrem
    mhigh =((burned_pixels >=440) & (burned_pixels <660)).sum()*30*30*acrem
    high = ((burned_pixels >=660)).sum()*30*30*acrem

    print('total acres within perimeter', total_within_contour)
    print('total burned acres', total)
    print('low severity', low/total)
    print('mlow severity', mlow/total)
    print('mhigh severity', mhigh/total )
    print('high severity',  high/total  )
    print('not burned', (burned_pixels<100).sum()*30*30*acrem/total)

    # Compute burn acreage estimation percent error
    actual = 132127
    ac_err = (total-actual)/actual*100

    print('Acreage estimation percent error: %0.2f' %ac_err)


.. parsed-literal::

    ('total acres within perimeter', 147159.10783350002)
    ('total burned acres', 112376.830428)
    ('low severity', 0.26375607555055969)
    ('mlow severity', 0.19382985292022228)
    ('mhigh severity', 0.24241842534395139)
    ('high severity', 0.29999564618526675)
    ('not burned', 0.30951466839763792)
    Acreage estimation percent error: -14.95


.. code:: ipython2

    # Plot these relative stats as a pie chart using pylab

    acreages = [low/total*100, mlow/total*100, mhigh/total*100, high/total*100]
    labels = ['Low', 'Moderate-low', 'Moderate-high', 'High']
    explode=(0, 0, 0, 0.05)
    facecolor = ['green', 'yellow', 'orange', 'red']
    pie(acreages,  explode=explode, labels=labels, colors = facecolor,
                     autopct='%.1f%%',shadow=True, startangle=90)

    title('Burn severity area fractions')
    fname = 'soberanes_pie_chart.png'
    plt.savefig(fname, bbox_inches='tight')



.. image:: https://cdn.descarteslabs.com/descarteslabs-python/wildfire_assessment_demo_files/wildfire_assessment_demo_19_0.png
