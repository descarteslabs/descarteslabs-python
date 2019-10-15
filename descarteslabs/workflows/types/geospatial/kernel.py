from ...cereal import serializable
from ..primitives import Int, Float
from ..containers import List, Struct, Tuple

KernelBase = Struct[{"dims": Tuple[Int, Int], "data": List[Float]}]


@serializable(is_named_concrete_type=True)
class Kernel(KernelBase):
    """
    A Kernel is a proxy object holding the kernel when performing a 2-dimensional
    convolution.
    """

    _doc = {
        "dims": "Tuple containing the dimensions of the kernel",
        "data": "List containing the kernel data in row-major format",
    }
    _constructor = "Kernel.load"
