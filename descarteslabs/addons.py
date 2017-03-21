class ThirdParty(object):
    _package = None

    def __init__(self, package):
        self._package = package

    def __getattr__(self, name):
        raise ImportError("Please install the %s package" % self._package)

    def __dir__(self):
        raise ImportError("Please install the %s package" % self._package)

    def __call__(self, *args, **kwargs):
        raise ImportError("Please install the %s package" % self._package)


try:
    from features import FeatureArray
except:
    FeatureArray = ThirdParty("descarteslabs-features")

try:
    import numpy
except:
    numpy = ThirdParty("numpy")
