from .api import patch_api
from .registry import patch_registry


def patch_all():
    patch_api()
    patch_registry()
