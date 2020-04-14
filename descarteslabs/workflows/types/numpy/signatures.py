from typing import Union
from inspect import Parameter, Signature

from ..primitives import Float, Int, Bool, NoneType
from ..array import Array, MaskedArray
from ..containers import List, Tuple


EMPTY = Parameter.empty
Param = lambda name, annotation=None, default=EMPTY, kind=Parameter.POSITIONAL_OR_KEYWORD: Parameter(  # noqa: E731
    name, kind=kind, default=default, annotation=annotation
)
Sig = lambda parameters, return_annotation: Signature(  # noqa: E731
    parameters=parameters, return_annotation=return_annotation
)

NUMPY_SIGNATURES = {
    "argmin": [
        Sig([Param("a", Union[Array, MaskedArray]), Param("axis", Int, None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)], Int
        ),
    ],
    "argmax": [
        Sig([Param("a", Union[Array, MaskedArray]), Param("axis", Int, None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)], Int
        ),
    ],
    "all": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)], Bool
        ),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
    ],
    "any": [
        Sig([Param("a", Array), Param("axis", Union[Int, List[Int]], None)], Array),
        Sig(
            [Param("a", Union[Array, MaskedArray]), Param("axis", NoneType, None)], Bool
        ),
        Sig(
            [Param("a", MaskedArray), Param("axis", Union[Int, List[Int]], None)],
            MaskedArray,
        ),
    ],
    "stack": [
        Sig([Param("arrays", List[Array]), Param("axis", Int, 0)], Array),
        Sig([Param("arrays", List[MaskedArray]), Param("axis", Int, 0)], MaskedArray),
    ],
    "concatenate": [
        Sig([Param("arrays", List[Array]), Param("axis", Int, 0)], Array),
        Sig([Param("arrays", List[MaskedArray]), Param("axis", Int, 0)], MaskedArray),
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
    "reshape": [
        Sig([Param("a", Array), Param("newshape", List[Int])], Array),
        Sig([Param("a", MaskedArray), Param("newshape", List[Int])], MaskedArray),
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
}
