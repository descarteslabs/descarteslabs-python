.. _numpy-functions:

Numpy Functions
---------------
Workflows provides equivalents to some NumPy functions through the ``workflows.numpy`` module, for use on the Workflows :class:`Array <descarteslabs.workflows.types.array.Array>` type. Supported functions are listed here. For more information, see the :ref:`Workflows guide <workflows-numpy-guide>`.

Use of these docstrings is subject to the `NumPy license <https://numpy.org/license.html>`_.

.. currentmodule:: descarteslabs.workflows.types.numpy

Top Level Functions
~~~~~~~~~~~~~~~~~~~

.. autosummary::
    absolute
    add
    all
    allclose
    angle
    any
    arange
    arccos
    arccosh
    arcsin
    arcsinh
    arctan
    arctan2
    arctanh
    argmax
    argmin
    argwhere
    around
    array
    atleast_1d
    atleast_2d
    atleast_3d
    average
    bincount
    broadcast_arrays
    broadcast_to
    cbrt
    ceil
    clip
    compress
    concatenate
    conj
    conjugate
    copysign
    corrcoef
    cos
    cosh
    count_nonzero
    cov
    cumprod
    cumsum
    deg2rad
    degrees
    diag
    diagonal
    diff
    digitize
    divide
    dot
    dstack
    ediff1d
    einsum
    equal
    exp
    exp2
    expm1
    eye
    fabs
    fix
    flatnonzero
    float_power
    floor
    floor_divide
    fmax
    fmin
    fmod
    full
    full_like
    gradient
    greater
    greater_equal
    histogram
    hstack
    imag
    indices
    insert
    isclose
    isfinite
    isin
    isinf
    isnan
    isreal
    less
    less_equal
    linspace
    log
    log10
    log1p
    log2
    logaddexp
    logaddexp2
    logical_and
    logical_not
    logical_or
    logical_xor
    max
    maximum
    mean
    meshgrid
    min
    minimum
    mod
    moveaxis
    multiply
    nan_to_num
    nanargmax
    nanargmin
    nancumprod
    nancumsum
    nanmax
    nanmean
    nanmin
    nanprod
    nanstd
    nansum
    nanvar
    negative
    nextafter
    nonzero
    not_equal
    ones
    ones_like
    outer
    pad
    percentile
    power
    prod
    ptp
    rad2deg
    radians
    ravel
    real
    reciprocal
    remainder
    repeat
    reshape
    rint
    roll
    rollaxis
    round
    sign
    signbit
    sin
    sinh
    spacing
    sqrt
    square
    squeeze
    stack
    std
    subtract
    sum
    take
    tan
    tanh
    tensordot
    tile
    trace
    transpose
    tril
    triu
    true_divide
    trunc
    unique
    unravel_index
    var
    vdot
    vstack
    where
    zeros
    zeros_like

Linear Algebra
~~~~~~~~~~~~~~

.. autosummary::
    linalg.cholesky
    linalg.inv
    linalg.lstsq
    linalg.norm
    linalg.qr
    linalg.solve
    linalg.svd

Masked Arrays
~~~~~~~~~~~~~

.. autosummary::
    ma.average
    ma.filled
    ma.fix_invalid
    ma.masked_array
    ma.masked_equal
    ma.masked_greater
    ma.masked_greater_equal
    ma.masked_inside
    ma.masked_invalid
    ma.masked_less
    ma.masked_less_equal
    ma.masked_not_equal
    ma.masked_outside
    ma.masked_values
    ma.masked_where

.. autofunction:: absolute
.. autofunction:: add
.. autofunction:: all
.. autofunction:: allclose
.. autofunction:: angle
.. autofunction:: any
.. autofunction:: arange
.. autofunction:: arccos
.. autofunction:: arccosh
.. autofunction:: arcsin
.. autofunction:: arcsinh
.. autofunction:: arctan
.. autofunction:: arctan2
.. autofunction:: arctanh
.. autofunction:: argmax
.. autofunction:: argmin
.. autofunction:: argwhere
.. autofunction:: around
.. autofunction:: array
.. autofunction:: atleast_1d
.. autofunction:: atleast_2d
.. autofunction:: atleast_3d
.. autofunction:: average
.. autofunction:: bincount
.. autofunction:: broadcast_arrays
.. autofunction:: broadcast_to
.. autofunction:: cbrt
.. autofunction:: ceil
.. autofunction:: clip
.. autofunction:: compress
.. autofunction:: concatenate
.. autofunction:: conj
.. autofunction:: conjugate
.. autofunction:: copysign
.. autofunction:: corrcoef
.. autofunction:: cos
.. autofunction:: cosh
.. autofunction:: count_nonzero
.. autofunction:: cov
.. autofunction:: cumprod
.. autofunction:: cumsum
.. autofunction:: deg2rad
.. autofunction:: degrees
.. autofunction:: diag
.. autofunction:: diagonal
.. autofunction:: diff
.. autofunction:: digitize
.. autofunction:: divide
.. autofunction:: dot
.. autofunction:: dstack
.. autofunction:: ediff1d
.. autofunction:: einsum
.. autofunction:: equal
.. autofunction:: exp
.. autofunction:: exp2
.. autofunction:: expm1
.. autofunction:: eye
.. autofunction:: fabs
.. autofunction:: fix
.. autofunction:: flatnonzero
.. autofunction:: float_power
.. autofunction:: floor
.. autofunction:: floor_divide
.. autofunction:: fmax
.. autofunction:: fmin
.. autofunction:: fmod
.. autofunction:: full
.. autofunction:: full_like
.. autofunction:: gradient
.. autofunction:: greater
.. autofunction:: greater_equal
.. autofunction:: histogram
.. autofunction:: hstack
.. autofunction:: imag
.. autofunction:: indices
.. autofunction:: insert
.. autofunction:: isclose
.. autofunction:: isfinite
.. autofunction:: isin
.. autofunction:: isinf
.. autofunction:: isnan
.. autofunction:: isreal
.. autofunction:: less
.. autofunction:: less_equal
.. autofunction:: linspace
.. autofunction:: log
.. autofunction:: log10
.. autofunction:: log1p
.. autofunction:: log2
.. autofunction:: logaddexp
.. autofunction:: logaddexp2
.. autofunction:: logical_and
.. autofunction:: logical_not
.. autofunction:: logical_or
.. autofunction:: logical_xor
.. autofunction:: max
.. autofunction:: maximum
.. autofunction:: mean
.. autofunction:: meshgrid
.. autofunction:: min
.. autofunction:: minimum
.. autofunction:: mod
.. autofunction:: moveaxis
.. autofunction:: multiply
.. autofunction:: nan_to_num
.. autofunction:: nanargmax
.. autofunction:: nanargmin
.. autofunction:: nancumprod
.. autofunction:: nancumsum
.. autofunction:: nanmax
.. autofunction:: nanmean
.. autofunction:: nanmin
.. autofunction:: nanprod
.. autofunction:: nanstd
.. autofunction:: nansum
.. autofunction:: nanvar
.. autofunction:: negative
.. autofunction:: nextafter
.. autofunction:: nonzero
.. autofunction:: not_equal
.. autofunction:: ones
.. autofunction:: ones_like
.. autofunction:: outer
.. autofunction:: pad
.. autofunction:: percentile
.. autofunction:: power
.. autofunction:: prod
.. autofunction:: ptp
.. autofunction:: rad2deg
.. autofunction:: radians
.. autofunction:: ravel
.. autofunction:: real
.. autofunction:: reciprocal
.. autofunction:: remainder
.. autofunction:: repeat
.. autofunction:: reshape
.. autofunction:: rint
.. autofunction:: roll
.. autofunction:: rollaxis
.. autofunction:: round
.. autofunction:: sign
.. autofunction:: signbit
.. autofunction:: sin
.. autofunction:: sinh
.. autofunction:: spacing
.. autofunction:: sqrt
.. autofunction:: square
.. autofunction:: squeeze
.. autofunction:: stack
.. autofunction:: std
.. autofunction:: subtract
.. autofunction:: sum
.. autofunction:: take
.. autofunction:: tan
.. autofunction:: tanh
.. autofunction:: tensordot
.. autofunction:: tile
.. autofunction:: trace
.. autofunction:: transpose
.. autofunction:: tril
.. autofunction:: triu
.. autofunction:: true_divide
.. autofunction:: trunc
.. autofunction:: unique
.. autofunction:: unravel_index
.. autofunction:: var
.. autofunction:: vdot
.. autofunction:: vstack
.. autofunction:: where
.. autofunction:: zeros
.. autofunction:: zeros_like

.. currentmodule:: descarteslabs.workflows.types.numpy.linalg

.. autofunction:: cholesky
.. autofunction:: inv
.. autofunction:: lstsq
.. autofunction:: norm
.. autofunction:: qr
.. autofunction:: solve
.. autofunction:: svd

.. currentmodule:: descarteslabs.workflows.types.numpy.ma

.. autofunction:: average
.. autofunction:: filled
.. autofunction:: fix_invalid
.. autofunction:: masked_array
.. autofunction:: masked_equal
.. autofunction:: masked_greater
.. autofunction:: masked_greater_equal
.. autofunction:: masked_inside
.. autofunction:: masked_invalid
.. autofunction:: masked_less
.. autofunction:: masked_less_equal
.. autofunction:: masked_not_equal
.. autofunction:: masked_outside
.. autofunction:: masked_values
.. autofunction:: masked_where
