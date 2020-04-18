from typing import Union
from inspect import Parameter, Signature

from ..primitives import Float, Int, Bool, NoneType
from ..array import Array, MaskedArray, Scalar, DType
from ..containers import List, Tuple


EMPTY = Parameter.empty
VAR_P = Parameter.VAR_POSITIONAL
VAR_KW = Parameter.VAR_KEYWORD

Param = lambda name, annotation=None, default=EMPTY, kind=Parameter.POSITIONAL_OR_KEYWORD: Parameter(  # noqa: E731
    name, kind=kind, default=default, annotation=annotation
)
Sig = lambda parameters, return_annotation: Signature(  # noqa: E731
    parameters=parameters, return_annotation=return_annotation
)

# TODO: Once interpret-time compute is available, change Scalar return types to
#   more specific types, if possible

NUMPY_SIGNATURES = {
    "all": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)],
            Scalar,  # TODO: Bool
        ),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
    ],
    "allclose": Sig(
        [
            Param("a", Union[Array, MaskedArray, Scalar]),
            Param("b", Union[Array, MaskedArray, Scalar]),
            Param("rtol", Union[Int, Float], 1e-05),
            Param("atol", Union[Int, Float], 1e-08),
            Param("equal_nan", Bool, False),
        ],
        Scalar,  # TODO: Bool
    ),
    "angle": [
        Sig([Param("z", Array), Param("deg", Bool, False)], Array),
        Sig([Param("z", MaskedArray), Param("deg", Bool, False)], Array),
    ],
    "any": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)],
            Scalar,  # TODO: Bool
        ),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
    ],
    "arange": [
        Sig([Param("stop", Union[Int, Float, Scalar])], Array),
        Sig(
            [
                Param("start", Union[Int, Float, Scalar]),
                Param("stop", Union[Int, Float, Scalar]),
                Param("step", Union[Int, Float, Scalar]),
            ],
            Array,
        ),
    ],
    "argmax": [
        Sig([Param("a", Union[Array, MaskedArray]), Param("axis", Int, None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)],
            Scalar,  # TODO: Int
        ),
    ],
    "argmin": [
        Sig([Param("a", Union[Array, MaskedArray]), Param("axis", Int, None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)],
            Scalar,  # TODO: Int
        ),
    ],
    "argwhere": Sig([Param("a", Union[Array, MaskedArray])], Array),
    "around": [
        Sig([Param("a", Array), Param("decimals", Int, 0)], Array),
        Sig([Param("a", MaskedArray), Param("decimals", Int, 0)], MaskedArray),
        Sig([Param("a", Scalar), Param("decimals", Int, 0)], Scalar),
    ],
    "atleast_1d": [
        Sig([Param("arys", Union[Array, Scalar])], Array),
        Sig([Param("arys", MaskedArray)], MaskedArray),
        Sig([Param("arys", Union[Array, Scalar], kind=VAR_P)], List[Array]),
        Sig([Param("arys", MaskedArray, kind=VAR_P)], List[MaskedArray]),
    ],
    "atleast_2d": [
        Sig([Param("arys", Union[Array, Scalar])], Array),
        Sig([Param("arys", MaskedArray)], MaskedArray),
        Sig([Param("arys", Union[Array, Scalar], kind=VAR_P)], List[Array]),
        Sig([Param("arys", MaskedArray, kind=VAR_P)], List[MaskedArray]),
    ],
    "atleast_3d": [
        Sig([Param("arys", Union[Array, Scalar])], Array),
        Sig([Param("arys", MaskedArray)], MaskedArray),
        Sig([Param("arys", Union[Array, Scalar], kind=VAR_P)], List[Array]),
        Sig([Param("arys", MaskedArray, kind=VAR_P)], List[MaskedArray]),
    ],
    "average": [
        Sig(
            [
                Param("a", Array),
                Param("axis", Union[Int, List[Int]], None),
                Param("weights", Union[Array, MaskedArray, Scalar], None),
            ],
            Array,
        ),
        Sig(
            [
                Param("a", MaskedArray),
                Param("axis", Union[Int, List[Int]], None),
                Param("weights", Union[Array, MaskedArray, Scalar], None),
            ],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
                Param("weights", Union[Array, MaskedArray, Scalar], None),
            ],
            Scalar,  # TODO: Float
        ),
    ],
    "bincount": Sig(
        [
            Param("x", Union[Array, MaskedArray]),
            Param("weights", Union[Array, MaskedArray, NoneType], None),
            Param("minlength", Int, 0),
        ],
        Array,
    ),
    "broadcast_arrays": [
        Sig([Param("args", Array, kind=VAR_P)], List[Array]),
        Sig([Param("args", MaskedArray, kind=VAR_P)], List[MaskedArray]),
    ],
    "broadcast_to": [
        Sig([Param("array", Array), Param("shape", List[Int])], Array),
        Sig([Param("array", MaskedArray), Param("shape", List[Int])], MaskedArray),
    ],
    "clip": [
        Sig(
            [
                Param("a", Array),
                Param("a_min", Union[Int, Float, Scalar, Array]),
                Param("a_max", Union[Int, Float, Scalar, Array]),
            ],
            Array,
        ),
        Sig(
            [
                Param("a", Array),
                Param("a_min", MaskedArray),
                Param("a_max", MaskedArray),
            ],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", MaskedArray),
                Param("a_min", Union[Int, Float, Scalar, Array, MaskedArray]),
                Param("a_max", Union[Int, Float, Scalar, Array, MaskedArray]),
            ],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Scalar),
                Param("a_min", Union[Int, Float, Scalar]),
                Param("a_max", Union[Int, Float, Scalar]),
            ],
            Scalar,
        ),
        Sig([Param("a", Scalar), Param("a_min", Array), Param("a_max", Array)], Array),
        Sig(
            [
                Param("a", Scalar),
                Param("a_min", MaskedArray),
                Param("a_max", MaskedArray),
            ],
            MaskedArray,
        ),
    ],
    "compress": [
        Sig(
            [
                Param("condition", Union[Array, MaskedArray]),
                Param("a", Array),
                Param("axis", Union[Int, NoneType], None),
            ],
            Array,
        ),
        Sig(
            [
                Param("condition", Union[Array, MaskedArray]),
                Param("a", MaskedArray),
                Param("axis", Union[Int, NoneType], None),
            ],
            MaskedArray,
        ),
    ],
    "concatenate": [
        Sig([Param("arrays", List[Array]), Param("axis", Int, 0)], Array),
        Sig([Param("arrays", List[MaskedArray]), Param("axis", Int, 0)], MaskedArray),
    ],
    "corrcoef": [
        Sig(
            [
                Param("x", Array),
                Param("y", Union[Array, NoneType], None),
                Param("rowvar", Bool, True),
            ],
            Array,
        ),
        Sig(
            [
                Param("x", Array),
                Param("y", MaskedArray, None),
                Param("rowvar", Bool, True),
            ],
            MaskedArray,
        ),
        Sig(
            [
                Param("x", MaskedArray),
                Param("y", Union[Array, MaskedArray, NoneType], None),
                Param("rowvar", Bool, True),
            ],
            MaskedArray,
        ),
    ],
    "count_nonzero": [
        Sig([Param("a", Array), Param("axis", Int, None)], Array),
        Sig([Param("a", MaskedArray), Param("axis", Int, None)], MaskedArray),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,
        ),
    ],
    "cov": [
        Sig(
            [
                Param("m", Array),
                Param("y", Union[Array, NoneType], None),
                Param("rowvar", Bool, True),
                Param("bias", Bool, False),
                Param("ddof", Union[Int, NoneType], None),
                Param("fweights", Union[Array, Int, NoneType], None),
                Param("aweights", Union[Array, NoneType], None),
            ],
            Array,
        ),
        Sig(
            [
                Param("m", Array),
                Param("y", MaskedArray, None),
                Param("rowvar", Bool, True),
                Param("bias", Bool, False),
                Param("ddof", Union[Int, NoneType], None),
                Param("fweights", Union[Array, Int, NoneType], None),
                Param("aweights", Union[Array, NoneType], None),
            ],
            MaskedArray,
        ),
        Sig(
            [
                Param("m", MaskedArray),
                Param("y", Union[Array, MaskedArray, NoneType], None),
                Param("rowvar", Bool, True),
                Param("bias", Bool, False),
                Param("ddof", Union[Int, NoneType], None),
                Param("fweights", Union[Array, Int, NoneType], None),
                Param("aweights", Union[Array, NoneType], None),
            ],
            MaskedArray,
        ),
    ],
    "cumprod": [
        Sig(
            [
                Param("a", Union[Array, Scalar]),
                Param("axis", Union[Int, NoneType], None),
                Param("dtype", Union[DType, NoneType], None),
            ],
            Array,
        ),
        Sig(
            [
                Param("a", MaskedArray),
                Param("axis", Union[Int, NoneType], None),
                Param("dtype", Union[DType, NoneType], None),
            ],
            MaskedArray,
        ),
    ],
    "cumsum": [
        Sig(
            [
                Param("a", Union[Array, Scalar]),
                Param("axis", Union[Int, NoneType], None),
                Param("dtype", Union[DType, NoneType], None),
            ],
            Array,
        ),
        Sig(
            [
                Param("a", MaskedArray),
                Param("axis", Union[Int, NoneType], None),
                Param("dtype", Union[DType, NoneType], None),
            ],
            MaskedArray,
        ),
    ],
    "histogram": Sig(
        [
            Param("a", Array),
            Param("bins", Union[List[Int], List[Float], Int], 10),
            Param("range", Union[Tuple[Int, Int], Tuple[Float, Float], NoneType], None),
            Param("weights", Union[Array, NoneType], None),
            Param("density", Union[Bool, NoneType], None),
        ],
        Tuple[Array, Array],
    ),
    "max": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,
        ),
    ],
    "mean": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,  # TODO: Float
        ),
    ],
    "min": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,
        ),
    ],
    "ptp": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,
        ),
    ],
    "reshape": [
        Sig([Param("a", Array), Param("newshape", List[Int])], Array),
        Sig([Param("a", MaskedArray), Param("newshape", List[Int])], MaskedArray),
    ],
    "stack": [
        Sig([Param("arrays", List[Array]), Param("axis", Int, 0)], Array),
        Sig([Param("arrays", List[MaskedArray]), Param("axis", Int, 0)], MaskedArray),
    ],
    "std": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,  # TODO: Float
        ),
    ],
    "sum": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
        Sig(
            [
                Param("a", Union[Array, MaskedArray, Scalar]),
                Param("axis", NoneType, None),
            ],
            Scalar,
        ),
    ],
    "transpose": [
        Sig(
            [Param("a", Array), Param("axes", Union[List[Int], NoneType], None)], Array
        ),
        Sig(
            [Param("a", MaskedArray), Param("axes", Union[List[Int], NoneType], None)],
            MaskedArray,
        ),
    ],
}


DISPLAY_SIGNATURE_OVERRIDES = {
    "arange": Sig(
        [
            Param("start", Union[Int, Float, Scalar], 0),
            Param("stop", Union[Int, Float, Scalar], None),
            Param("step", Union[Int, Float, Scalar], 1),
        ],
        Array,
    ),
    "atleast_1d": Sig(
        [Param("arys", Union[Array, MaskedArray, Scalar], kind=VAR_P)],
        Union[Array, MaskedArray, List[Array], List[MaskedArray]],
    ),
    "atleast_2d": Sig(
        [Param("arys", Union[Array, MaskedArray, Scalar], kind=VAR_P)],
        Union[Array, MaskedArray, List[Array], List[MaskedArray]],
    ),
    "atleast_3d": Sig(
        [Param("arys", Union[Array, MaskedArray, Scalar], kind=VAR_P)],
        Union[Array, MaskedArray, List[Array], List[MaskedArray]],
    ),
}
