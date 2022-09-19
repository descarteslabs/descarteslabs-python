"""
Note: It is recommended to use the object oriented interface at :py:mod:`descarteslabs.vectors`.
"""

from .vector import Vector
from ....common.property_filtering import Properties

properties = Properties()


__all__ = ["Vector", "properties"]
