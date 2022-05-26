class GraftError(Exception):
    pass


class GraftSyntaxError(GraftError):
    pass


class GraftNameError(GraftError):
    pass


class GraftTypeError(GraftError):
    pass


class GraftRuntimeError(GraftError):
    pass
