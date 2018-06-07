#!/usr/bin/env python

import numpy as np
import matplotlib
# avoid import errors on macOS
matplotlib.use("AGG")  # noqa
import matplotlib.pyplot as plt
import sys

import descarteslabs.scenes as scn

a = np.arange(20 * 15).reshape((20, 15))
b = np.tan(a)
scn.display(a, b, size=3)

plt.savefig(sys.argv[1])
