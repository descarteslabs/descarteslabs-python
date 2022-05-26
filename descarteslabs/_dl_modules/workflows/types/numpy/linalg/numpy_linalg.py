import numpy as np

from ..numpy_functions import np_func
from ..signatures import NUMPY_LINALG as NP_LINALG


np_linalg = {
    name: np_func(getattr(np.linalg, name), "linalg." + name, sigs)
    for name, sigs in NP_LINALG.items()
}
