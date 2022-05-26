import numpy as np


def masked_stack(arrays, axis=0):
    d = np.stack([np.ma.getdata(a) for a in arrays], axis)
    rcls = np.ma.core.get_masked_subclass(*arrays)
    data = d.view(rcls)

    # Check whether at least one of the arrays has a nonempty mask.
    for a in arrays:
        if np.ma.getmask(a) is not np.ma.nomask:
            break
    else:
        return data

    # If so, stack the masks.
    dm = np.stack([np.ma.getmaskarray(a) for a in arrays], axis)
    dm = dm.reshape(d.shape)

    # Try to shink the masks to `np.ma.nomask`, if possible.
    data._mask = np.ma.core._shrink_mask(dm)
    return data
