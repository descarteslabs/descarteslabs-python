# Copyright 2018-2020 Descartes Labs.
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

"""
Displays ndarrays as images, but is easier to use and more flexible than matplotlib's ``imshow``.
"""

from __future__ import division
import six

import descarteslabs.client.addons


def display(*imgs, **kwargs):
    """
    Display 2D and 3D ndarrays as images with matplotlib.

    The ndarrays must either be 2D, or 3D with 1 or 3 bands.
    If they are 3D masked arrays, the mask will be used as an alpha channel.

    Unlike matplotlib's ``imshow``, arrays can be any dtype;
    internally, each is normalized to the range [0..1].

    Parameters
    ----------
    *imgs: 1 or more ndarrays
        When multiple images are given, each is displayed on its own row.
    bands_axis: int, default 0
        Axis which contains bands in each array.
    title: str, or sequence of str; optional
        Title for each image. If a sequence, must be the same length as ``imgs``.
    size: int, default 10
        Length, in inches, to display the longer side of each image.
    robust: bool, default True
        Use the 2nd and 98th percentiles to compute color limits.
        Otherwise, the minimum and maximum values in each array are used.
    interpolation: str, default "bilinear"
        Interpolation method for matplotlib to use when scaling images for display.

        Bilinear is the default, since it produces smoother results when scaling
        down continuously-valued data (i.e. images). For displaying discrete data,
        however, choose 'nearest' to prevent values not existing in the input
        from appearing in the output.

        Acceptable values are 'none', 'nearest', 'bilinear', 'bicubic', 'spline16',
        'spline36', 'hanning', 'hamming', 'hermite', 'kaiser', 'quadric', 'catrom',
        'gaussian', 'bessel', 'mitchell', 'sinc', 'lanczos'
    colormap: str, default None
        The name of a Colormap registered with matplotlib. Some commonly used
        built-in options are 'plasma', 'magma', 'viridis', 'inferno'. See
        https://matplotlib.org/users/colormaps.html for more options.

        To use a Colormap, the input images must have a single band. The Colormap
        will be ignored for images with more than one band.

    Raises
    ------
    ImportError
        If matplotlib is not installed.
    """
    _display_or_save(None, *imgs, **kwargs)


def save_image(filename, *imgs, **kwargs):
    """
    Save 2D and 3D ndarrays as images with matplotlib.

    For an explanation of the rest of the arguments, please look under :func:`display`.
    For an explanation of valid extension types, please look under matplotlib
    :func:'savefig'.

    Parameters
    ----------
    filename: str
        The name and extension of the image to be saved.

    """
    _display_or_save(filename, *imgs, **kwargs)


def _display_or_save(filename, *imgs, **kwargs):
    if len(imgs) == 0:
        return

    bands_axis = kwargs.pop("bands_axis", 0)
    titles = kwargs.pop("title", None)
    size = kwargs.pop("size", 10)
    robust = kwargs.pop("robust", True)
    interpolation = kwargs.pop("interpolation", "bilinear")
    colormap_name = kwargs.pop("colormap", None)

    if len(kwargs) > 0:
        raise TypeError(
            "Unexpected keyword arguments for display: {}".format(
                ", ".join(six.iterkeys(kwargs))
            )
        )

    np = descarteslabs.client.addons.numpy
    matplotlib = descarteslabs.client.addons.import_matplotlib_pyplot()
    plt = matplotlib.pyplot

    if len(imgs) == 1:
        if isinstance(imgs[0], (list, tuple)):
            raise TypeError(
                "To display a sequence of images, unpack it: `display(*images_list)`"
            )
        elif isinstance(imgs[0], np.ndarray) and len(imgs[0].shape) == 4:
            raise TypeError(
                "To display a 4D ndarray (image stack), unpack it: `display(*stack)`"
            )

    # TODO: facet grid
    # TODO: leaves huge gaps between images that aren't very square
    # would need to calculate figsize better based on shapes of each img
    # or use seaborn?
    figsize = (size, size * len(imgs))
    fig, axs = plt.subplots(len(imgs), 1, figsize=figsize, squeeze=False)

    if isinstance(titles, (list, tuple, np.ndarray)):
        if len(titles) != len(imgs):
            raise ValueError("Different number of titles given than images")
    else:
        titles = [titles] * len(imgs)

    colormap = None
    if colormap_name:
        colormap = plt.cm.get_cmap(colormap_name)

    for ax, img, title in zip(axs, imgs, titles):
        ax = ax[0]

        if not isinstance(img, np.ndarray):
            raise TypeError("Expected ndarray, instead got {}".format(type(img)))

        if len(img.shape) not in (2, 3):
            raise NotImplementedError(
                "Can only display 2D or 3D arrays, not shape {}".format(img.shape)
            )

        if len(img.shape) == 2:
            # expand 2d image to 3d with 1 band
            slicer = [slice(None)] * 3
            slicer[bands_axis] = np.newaxis
            img = img[tuple(slicer)]

        nbands = img.shape[bands_axis]
        if nbands not in (1, 3, 4):
            raise NotImplementedError(
                "Can only display images with 1 or 3 bands currently, not {}. "
                "Is axis {} actually your bands axis?".format(nbands, bands_axis)
            )

        if nbands == 4:
            # don't include alpha band in min max
            slicer = [slice(None)] * 3
            slicer[bands_axis] = slice(None, 3)
            spectrals = img[tuple(slicer)]
        else:
            spectrals = img

        # calculate min and max
        if robust:
            if hasattr(spectrals, "mask"):
                spectrals = (
                    spectrals.compressed()
                )  # don't include masked values in percentile
            vmin, vmax = np.nanpercentile(spectrals, 2), np.nanpercentile(spectrals, 98)
            if vmin == vmax:
                robust = False

        if not robust:
            vmin, vmax = np.ma.min(spectrals), np.ma.max(spectrals)

        # matplotlib requires shape (n, m, band)
        disp = np.moveaxis(img, bands_axis, -1).astype(np.float64)

        # rescale
        disp -= vmin
        disp /= vmax - vmin
        np.clip(disp, 0, 1, out=disp)  # to coerce away any floating-point errors

        if hasattr(disp, "mask"):
            # turn mask into alpha
            if nbands == 4:
                raise NotImplementedError(
                    "Currently can't supply an image with an explicit alpha band as well as a mask"
                )
            if np.isscalar(disp.mask):
                alpha = np.ones(disp.shape[:2]) * (not disp.mask)
            else:
                alpha = (~disp.mask.any(axis=-1)).astype(disp.dtype)
            if nbands == 1:
                if colormap:
                    disp = colormap(disp[:, :, 0])
                    disp = disp[
                        :, :, :3
                    ]  # Removes the alpha channel the color map always adds
                else:
                    # to use an alpha channel, matplotlib must have a 4-band image,
                    # so just duplicate the 1 band for r, g, and b
                    disp = np.concatenate(
                        [disp] * 3, axis=-1
                    )  # TODO: unnecessary copy of disp's mask
            disp = np.concatenate([disp, alpha[:, :, np.newaxis]], axis=-1)

        if disp.shape[-1] == 1:
            # matplotlib takes 1 band images as (n, m), not (n, m, 1)
            disp = disp[:, :, 0]
            if colormap:
                disp = colormap(disp)

        ax.grid(False)  # just to be sure
        ax.imshow(disp, aspect="equal", interpolation=interpolation)
        if title is not None:
            ax.set_title(str(title))
    fig.tight_layout()

    if filename is not None:
        plt.savefig(filename)
    else:
        plt.show()
