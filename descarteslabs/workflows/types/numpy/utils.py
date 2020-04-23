import re
import numpy as np


def copy_docstring_from_numpy(wf_func, np_func):
    """
    Attaches `np_func`'s docstring to `wf_func` with modifications: examples,
    references, and See Also sections are removed, as well as unsupported
    parameters.

    Parameters
    ----------
    wf_func: ufunc or wf_func
        The class to attach the generated docstring to. Either a `ufunc` class
        (for handling ufuncs) or `wf_func` class (for handling everything else)

    np_func: numpy method
        The numpy method whose docstring will be copied. (ie. np.add)
    """

    doc = np_func.__doc__.replace("[1]_", "")
    doc = doc.replace(
        ":ref:`ufunc docs <ufuncs.kwargs>`.",
        "`ufunc docs <https://docs.scipy.org/doc/numpy/reference/ufuncs.html#ufuncs-kwargs>`_.",
    )
    doc = doc.replace(
        ":ref:`routines.linalg-broadcasting`",
        "`Linear algebra on several matrices at once <https://docs.scipy.org/"
        "doc/numpy/reference/routines.linalg.html#routines-linalg-broadcasting>`_.",
    )

    # replace asterisks used in bulleted lists with a dash
    doc = re.sub(r" \* ([\w',]+\s[\w]+)", r" - \1", doc)

    # remove "Examples"
    doc = doc.split("\n\n    Examples\n")[0]

    # remove "References"
    doc = [a for a in doc.split("\n\n") if "References\n" not in a]

    # remove "See Also" section
    doc = [a for a in doc if "See Also\n" not in a]

    # remove "Raises" section
    doc = [a for a in doc if "Raises\n" not in a]

    l1 = "This docstring was copied from ``numpy.{}``".format(np_func.__name__)
    l2 = "Some inconsistencies with the Workflows version may exist"

    if isinstance(np_func, np.ufunc):
        # what the function does
        info = doc[1]

        # parameters (sometimes listed on separate lines, sometimes not)
        parameters = [a for a in doc if "Parameters\n" in a][0].split("\n")
        if parameters[4][0] == "x":
            parameters = "\n".join(parameters[:6])
        else:
            parameters = "\n".join(parameters[:4])

        # return value
        returns = [a for a in doc if "Returns\n" in a][0]

        # final docstring
        doc = "\n\n".join([info, l1, l2, parameters, returns])
    else:
        # does the first line contain the function signature? (not always the case)
        if doc[0][-1] == ")":
            doc = [doc[1]] + ["\n\n" + "    {}\n\n    {}\n\n".format(l1, l2)] + doc[2:]
        else:
            doc = [doc[0]] + ["\n\n" + "    {}\n\n    {}\n\n".format(l1, l2)] + doc[1:]
        doc = "\n\n".join(doc)

    wf_func.__doc__ = doc
