"""
Helper module that exposes ipywidgets if it's available, otherwise a MagicMock instance.

Allows parameter widgets to be imported and used without optional dependencies---they just won't show up as widgets.
"""

try:
    import ipywidgets

    have_ipywidgets = True
except ImportError:
    from unittest import mock

    ipywidgets = mock.MagicMock()
    have_ipywidgets = False
