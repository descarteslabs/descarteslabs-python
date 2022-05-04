from __future__ import division

import pytest
import unittest
import mock

import numpy as np
from .. import _display
from .. import display


class TestDisplay(unittest.TestCase):
    @staticmethod
    def make_mock_subplots(mock_matplotlib_importer, n_imgs):
        mock_matplotlib = mock.Mock()
        mock_matplotlib_importer.return_value = mock_matplotlib

        mock_plt = mock_matplotlib.pyplot
        mock_fig = mock.Mock()
        mock_axs = [[mock.Mock()] for i in range(n_imgs)]

        mock_plt.subplots.return_value = (mock_fig, mock_axs)
        return mock_plt, mock_fig, mock_axs

    @mock.patch.object(_display, "import_matplotlib_pyplot")
    def test_display_2d(self, mock_matplotlib_importer):
        mock_plt, mock_fig, mock_axs = self.make_mock_subplots(
            mock_matplotlib_importer, 1
        )

        img = np.arange(6).reshape((3, 2))

        img_normed = img / (img.size - 1)
        display(img, size=5, title="foo", robust=False)

        mock_plt.subplots.assert_called_with(1, 1, figsize=(5, 5), squeeze=False)

        ax = mock_axs[0][0]
        imshow_args, imshow_kwargs = ax.imshow.call_args
        assert (imshow_args[0] == img_normed).all()
        ax.set_title.assert_called_with("foo")

    @mock.patch.object(_display, "import_matplotlib_pyplot")
    def test_display_3d_masked(self, mock_matplotlib_importer):
        mock_plt, mock_fig, mock_axs = self.make_mock_subplots(
            mock_matplotlib_importer, 1
        )

        img = np.arange(3 * 3 * 2).reshape((3, 3, 2))
        mask = np.zeros_like(img).astype(bool)
        mask[0, 0, 0] = True
        mask[2, 0, 0] = True
        mask[:, -1, -1] = True
        img = np.ma.MaskedArray(img, mask=mask)

        alpha = np.ones((3, 2), dtype=float)
        alpha[0, 0] = 0
        alpha[-1, -1] = 0

        display(img)

        ax = mock_axs[0][0]
        imshow_args, imshow_kwargs = ax.imshow.call_args
        called_arr = imshow_args[0]
        assert called_arr.shape == (3, 2, 4)
        assert (called_arr[:, :, -1] == alpha).all()

        ax.set_title.assert_not_called()

    @mock.patch.object(_display, "import_matplotlib_pyplot")
    def test_display_3d_multiple(self, mock_matplotlib_importer):
        mock_plt, mock_fig, mock_axs = self.make_mock_subplots(
            mock_matplotlib_importer, 5
        )

        img = np.arange(len(mock_axs) * 3 * 3 * 2).reshape((len(mock_axs), 3, 3, 2))
        with pytest.raises(TypeError, match="To display a 4D ndarray"):
            display(img)

        display(*img, title=list(range(len(img))))

        for i, ax in enumerate(mock_axs):
            ax = ax[0]
            imshow_args, imshow_kwargs = ax.imshow.call_args
            called_arr = imshow_args[0]
            assert called_arr.shape == (3, 2, 3)
            ax.set_title.assert_called_with(str(i))

    @mock.patch.object(_display, "import_matplotlib_pyplot")
    def test_fails_2band(self, mock_matplotlib_importer):
        mock_plt, mock_fig, mock_axs = self.make_mock_subplots(
            mock_matplotlib_importer, 1
        )

        img = np.arange(2 * 4 * 2).reshape((2, 4, 2))

        with pytest.raises(NotImplementedError):
            display(img)

    @mock.patch.object(_display, "import_matplotlib_pyplot")
    def test_fails_wrong_num_titles(self, mock_matplotlib_importer):
        mock_plt, mock_fig, mock_axs = self.make_mock_subplots(
            mock_matplotlib_importer, 5
        )

        img = np.arange(len(mock_axs) * 3 * 3 * 2).reshape((len(mock_axs), 3, 3, 2))
        with pytest.raises(ValueError, match="titles"):
            display(*img, title=[1, 2])

    @mock.patch.object(_display, "import_matplotlib_pyplot")
    def test_fails_wrong_kwargs(self, mock_matplotlib_importer):
        with pytest.raises(TypeError, match="what"):
            display(None, title="foo", what="bar")
