import logging

import matplotlib.pyplot as plt
from descarteslabs import metadata, raster
from descarteslabs.ext.cache import cached
from descarteslabs.ext.storage import Storage

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
client = Storage()


@cached(client)
def load_data(dltile):
    ids = metadata.ids(
        dltile=dltile, products=['landsat:LC08:01:RT:TOAR'],
        start_datetime='2017-08-01', end_datetime='2017-08-16'
    )
    arrs = []
    for id_ in ids:
        arr, meta = raster.ndarray(
            id_,
            bands=['red', 'green', 'blue', 'nir', 'swir1', 'alpha'],
            dltile=dltile,
            order='gdal'
        )
        arrs.append(arr)
    return arrs


@cached(client)
def calculate_band_ratio(dltile):
    arrs = load_data(dltile)
    ndvis = []
    for arr in arrs:
        ndvi = (arr[3] - arr[0]) / (1.0e-6 + arr[3] + arr[0])
        ndvis.append(ndvi)
    return ndvis


def plot():
    dltile = raster.dltile_from_latlon(45, -90, 15.0, 2048, 0)
    ndvis = calculate_band_ratio(dltile)

    for i, ndvi in enumerate(ndvis):
        plt.imsave('ndvi_{:>04}.png'.format(i), ndvi, cmap='Spectral')


if __name__ == "__main__":
    plot()
