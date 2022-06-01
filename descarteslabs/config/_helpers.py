import importlib
import sys
import types

from . import get_settings


# A special module Finder implementation which can map from _dl_modules while
# also ensuring that the environment is configured
class DescartesLabsFinder(importlib.abc.MetaPathFinder):
    def __init__(self, prefix="descarteslabs"):
        self._prefix = f"{prefix}."
        self._module_prefix = f"{prefix}._dl_modules."

    # MetaPathFinder API
    def find_spec(self, fullname, path, module=None):
        # avoid recursion on non-existent module
        if fullname.startswith(self._prefix) and not fullname.startswith(
            self._module_prefix
        ):
            module = None
            loader = None
            # auto-configure if not already configured
            settings = get_settings()
            # double check sys.modules in case our target was just auto-configured
            module = sys.modules.get(fullname, None)
            if module:
                # auto-configure has just created it. The modules will be
                # set up for final consumption, so create a loader which
                # will just reuse the module
                loader = DescartesLabsLoader(module, True)
                # prevent import machinery from short-circuiting, it will be reinserted later
                sys.modules.pop(fullname)
            elif not settings.get("AWS_CLIENT", False):
                # the AWS client has everything fully exposed, but the
                # gcp client has many modules which are not part of the
                # top level, and which should always be accessible.
                dlname = f"{self._module_prefix}{fullname[len(self._prefix):]}"
                try:
                    module = importlib.import_module(dlname)
                except ImportError:
                    pass
                if module:
                    # create a loader which will map the _dl_modules module
                    # into the descarteslabs package structure
                    loader = DescartesLabsLoader(module, False)
            if loader:
                # construct a new spec like the original
                spec = loader.create_spec(fullname)
                return spec
        return None


# Special loader to assist with config-based importation
class DescartesLabsLoader(importlib.abc.Loader):
    # module attributes which should not be copied
    MODULE_STATE = set(
        [
            "__builtins__",
            "__loader__",
            "__name__",
            # "__package__",  This must be copied to preserve relative imports
            "__path__",
            "__spec__",
        ]
    )

    def __init__(self, module, reuse_module=False):
        self._module = module
        self._reuse_module = reuse_module

    # not part of the API but useful for us
    def create_spec(self, name):
        # construct a new spec like the original
        origin = self._module.__spec__.origin
        spec = importlib.machinery.ModuleSpec(name, loader=self, origin=origin)
        if self._module.__spec__.submodule_search_locations is not None:
            # a list is required to make the import mechanism think it is a package,
            # however we don't want it to find any files or dirs, so it will make a
            # new import request for each subpackage that will trap through here again.
            spec.submodule_search_locations = []
        return spec

    # Loader API to create the new module
    def create_module(self, spec):
        if self._reuse_module:
            return self._module
        else:
            module = types.ModuleType(spec.name)
            module.__spec__ = spec
            return module

    # Loader API to actually load the module contents
    def exec_module(self, module):
        # copy everything from the source module to the dest module, except module state itself
        if module is not self._module:
            for attr in dir(self._module):
                if attr not in self.MODULE_STATE:
                    setattr(module, attr, getattr(self._module, attr))


# A wrapper fake module for the top-level `descarteslabs` which will proxy
# everything through the real module after ensuring that we are configured.
# For why this works, see https://stackoverflow.com/questions/2447353/getattr-on-a-module
class DescartesLabsModule(object):
    def __init__(self, module):
        self._module = module

    def __getattr__(self, attr):
        # ensure we are configured
        try:
            return getattr(self._module, attr)
        except AttributeError:
            get_settings()
            return getattr(self._module, attr)

    def __dir__(self):
        return self._module.__dir__()


# A wrapper fake module for unsupported client modules.
# For why this works, see https://stackoverflow.com/questions/2447353/getattr-on-a-module
class DescartesLabsUnsupportedModule(object):
    def __init__(self, name, message, exc):
        self.__name__ = name
        self._message = message
        self._exc = exc

    def __getattr__(self, attr):
        if attr.startswith("__"):
            return super().__getattribute__(attr)

        raise ImportError(self._message) from self._exc

    def __dir__(self):
        return self._module.__dir__()


# this function will take a real module (e.g. descarteslabs._dl_modules.whatever)
# and create a new module which will inherit everything in a way compatible with
# the Finder above. The name should be the new name (e.g. descarteslabs.whatever).
def clone_module(name, module):
    loader = DescartesLabsLoader(module, False)
    clone = loader.create_module(loader.create_spec(name))
    loader.exec_module(clone)
    return clone
