import importlib
import sys

import lazy_object_proxy

import descarteslabs
import descarteslabs.auth

from ._helpers import DescartesLabsUnsupportedModule, clone_module


def _setup_gcp():
    version = importlib.import_module("descarteslabs._dl_modules.client.version")
    setattr(descarteslabs, "__version__", version.__version__)

    setattr(descarteslabs, "Auth", descarteslabs.auth.Auth)

    dl_client = importlib.import_module("descarteslabs._dl_modules.client")
    client = clone_module("descarteslabs.client", dl_client)
    setattr(descarteslabs, "client", client)
    sys.modules[client.__name__] = client

    dl_services = importlib.import_module("descarteslabs._dl_modules.client.services")
    services = clone_module("descarteslabs.client.services", dl_services)
    setattr(client, "services", services)
    setattr(descarteslabs, "services", services)
    sys.modules[services.__name__] = services

    dl_metadata = importlib.import_module(
        "descarteslabs._dl_modules.client.services.metadata"
    )
    metadata = clone_module("descarteslabs.client.services.metadata", dl_metadata)
    setattr(services, "metadata", metadata)
    setattr(services, "Metadata", metadata.Metadata)
    setattr(descarteslabs, "Metadata", metadata.Metadata)
    sys.modules[metadata.__name__] = metadata

    dl_places = importlib.import_module(
        "descarteslabs._dl_modules.client.services.places"
    )
    places = clone_module("descarteslabs.client.services.places", dl_places)
    setattr(services, "places", places)
    setattr(services, "Places", places.Places)
    setattr(descarteslabs, "Places", places.Places)
    sys.modules[places.__name__] = places

    dl_raster = importlib.import_module(
        "descarteslabs._dl_modules.client.services.raster"
    )
    raster = clone_module("descarteslabs.client.services.raster", dl_raster)
    setattr(services, "raster", raster)
    setattr(services, "Raster", raster.Raster)
    setattr(descarteslabs, "Raster", raster.Raster)
    sys.modules[raster.__name__] = raster

    dl_storage = importlib.import_module(
        "descarteslabs._dl_modules.client.services.storage"
    )
    storage = clone_module("descarteslabs.client.services.storage", dl_storage)
    setattr(services, "storage", storage)
    setattr(services, "Storage", storage.Storage)
    setattr(descarteslabs, "Storage", storage.Storage)
    sys.modules[storage.__name__] = storage

    dl_catalog = importlib.import_module(
        "descarteslabs._dl_modules.client.services.catalog"
    )
    catalog = clone_module("descarteslabs.client.services.catalog", dl_catalog)
    setattr(services, "catalog", catalog)
    setattr(services, "Catalog", catalog.Catalog)
    setattr(descarteslabs, "Catalog", catalog.Catalog)
    sys.modules[catalog.__name__] = catalog

    dl_tasks = importlib.import_module(
        "descarteslabs._dl_modules.client.services.tasks"
    )
    tasks = clone_module("descarteslabs.client.services.tasks", dl_tasks)
    setattr(services, "tasks", tasks)
    setattr(services, "AsyncTasks", tasks.AsyncTasks)
    setattr(descarteslabs, "AsyncTasks", tasks.AsyncTasks)
    setattr(services, "CloudFunction", tasks.CloudFunction)
    setattr(descarteslabs, "CloudFunction", tasks.CloudFunction)
    setattr(services, "FutureTask", tasks.FutureTask)
    setattr(descarteslabs, "FutureTask", tasks.FutureTask)
    setattr(services, "Tasks", tasks.Tasks)
    setattr(descarteslabs, "Tasks", tasks.Tasks)
    sys.modules[tasks.__name__] = tasks

    dl_vector = importlib.import_module(
        "descarteslabs._dl_modules.client.services.vector"
    )
    vector = clone_module("descarteslabs.client.services.vector", dl_vector)
    setattr(services, "vector", vector)
    setattr(services, "Vector", vector.Vector)
    setattr(descarteslabs, "Vector", vector.Vector)
    sys.modules[vector.__name__] = vector

    services.__all__ = [
        "AsyncTasks",
        "Catalog",
        "CloudFunction",
        "FutureTask",
        "Metadata",
        "Places",
        "Raster",
        "Storage",
        "Tasks",
        "Vector",
    ]

    dl_common = importlib.import_module("descarteslabs._dl_modules.common")
    common = clone_module("descarteslabs.common", dl_common)
    setattr(descarteslabs, "common", common)
    sys.modules[common.__name__] = common

    dl_property_filtering = importlib.import_module(
        "descarteslabs._dl_modules.common.property_filtering"
    )
    property_filtering = clone_module(
        "descarteslabs.common.property_filtering", dl_property_filtering
    )
    setattr(common, "property_filtering", property_filtering)
    setattr(descarteslabs, "GenericProperties", property_filtering.GenericProperties)
    properties = property_filtering.GenericProperties()
    setattr(descarteslabs, "properties", properties)
    sys.modules[property_filtering.__name__] = property_filtering

    descartes_auth = lazy_object_proxy.Proxy(descarteslabs.auth.Auth.get_default_auth)
    setattr(descarteslabs, "descartes_auth", descartes_auth)
    setattr(
        descarteslabs,
        "raster",
        lazy_object_proxy.Proxy(lambda: raster.Raster(auth=descartes_auth)),
    )
    setattr(
        descarteslabs,
        "storage",
        lazy_object_proxy.Proxy(lambda: storage.Storage(auth=descartes_auth)),
    )
    setattr(
        descarteslabs,
        "tasks",
        lazy_object_proxy.Proxy(lambda: tasks.Tasks(auth=descartes_auth)),
    )
    setattr(
        descarteslabs,
        "metadata",
        lazy_object_proxy.Proxy(lambda: metadata.Metadata(auth=descartes_auth)),
    )
    setattr(
        descarteslabs,
        "places",
        lazy_object_proxy.Proxy(lambda: places.Places(auth=descartes_auth)),
    )
    setattr(
        descarteslabs,
        "vector",
        lazy_object_proxy.Proxy(lambda: vector.Vector(auth=descartes_auth)),
    )

    # these should be fully imported and in the local namespace
    for package in ("catalog", "scenes", "vectors"):
        dl_module = importlib.import_module(f"descarteslabs._dl_modules.{package}")
        module = clone_module(f"descarteslabs.{package}", dl_module)
        setattr(descarteslabs, package, module)
        sys.modules[module.__name__] = module

    # tentatively import tables (depends on extras)
    try:
        dl_tables = importlib.import_module("descarteslabs._dl_modules.tables")
        tables = clone_module("descarteslabs.tables", dl_tables)
    except ImportError as e:
        tables = DescartesLabsUnsupportedModule(
            "tables",
            """descarteslabs.tables not supported due to missing optional depencencies
Please install optional dependencies using:
$ pip install --upgrade 'descarteslabs[tables]'
""",
            e,
        )
    setattr(descarteslabs, "tables", tables)
    sys.modules[tables.__name__] = tables

    descarteslabs.__all__.extend(
        [
            "__version__",
            "AsyncTasks",
            "Auth",
            "Catalog",
            "CloudFunction",
            "FutureTask",
            "Metadata",
            "Places",
            "Raster",
            "Storage",
            "Tasks",
            "Vector",
            "catalog",
            "descartes_auth",
            "exceptions",
            "metadata",
            "places",
            "properties",
            "raster",
            "scenes",
            "services",
            "storage",
            "tables",
            "tasks",
            "vector",
            "vectors",
        ]
    )
