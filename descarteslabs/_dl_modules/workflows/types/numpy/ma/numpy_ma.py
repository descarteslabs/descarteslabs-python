import numpy as np

from ..numpy_functions import np_func
from ..signatures import NUMPY_MA as NP_MA

np_ma = {
    name: np_func(getattr(np.ma, name), "ma." + name, sigs)
    for name, sigs in NP_MA.items()
}
