from collections import ChainMap


class ScopedChainMap(ChainMap):
    def getlevel(self, k, default_value=None, default_level=None):
        "Look up a key and the level where it's stored, returning defaults if it doesn't exist"
        for i, mapping in enumerate(self.maps):
            try:
                return mapping[k], i
            except KeyError:
                pass
        return default_value, default_level

    def set(self, k, v, non_local=True):
        "Set `k` to `v`, at the scope level where `k` is already defined if `non_local`"
        if non_local:
            for mapping in self.maps:
                if k in mapping:
                    mapping[k] = v
                    return
        self.maps[0][k] = v

    def setlevel(self, k, v, level=0):
        "Set `k` to `v` at `level`"
        self.maps[level][k] = v
