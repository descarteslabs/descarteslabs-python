ENTRYPOINT = "__dlentrypoint__.py"
DIST = "dist"
DATA = "data"
REQUIREMENTS = "requirements.txt"


class FunctionType(object):
    PY_PICKLE = "py_pickle"
    PY_BUNDLE = "py_bundle"

    ALL = (PY_PICKLE, PY_BUNDLE)
