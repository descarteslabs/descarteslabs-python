"""
Example:
    In [1]: from descarteslabs.tables import Tables
       ...: import ibis
       ...: import shapely.geometry
       ...: s = shapely.geometry.shape({
       ...:          "type": "Polygon",
       ...:          "coordinates": [
       ...:            [
       ...:              [
       ...:                -70.32142639160156,
       ...:                43.63657210502274
       ...:              ],
       ...:              [
       ...:                -70.23405075073242,
       ...:                43.63657210502274
       ...:              ],
       ...:              [
       ...:                -70.23405075073242,
       ...:                43.691956147956965
       ...:              ],
       ...:              [
       ...:                -70.32142639160156,
       ...:                43.691956147956965
       ...:              ],
       ...:              [
       ...:                -70.32142639160156,
       ...:                43.63657210502274
       ...:              ]
       ...:            ]
       ...:          ]
       ...: })
       ...: tables = Tables()
       ...: t = tables.table("whosonfirst_v2")
       ...: q = t[t.wof_name, t.geom]
       ...: q = q.filter(q.geom.set_srid(4326).intersects(ibis.literal(s)))
       ...: q = q.limit(10)
       ...: df = q.execute()
       ...: df
       ...:
    Out[1]:
                             geom         wof_name
    0  POINT (-70.29025 43.65688)        Libbytown
    1  POINT (-70.31338 43.67381)    Nasons Corner
    2  POINT (-70.29063 43.66753)        Highlands
    3  POINT (-70.29609 43.66824)  Brighton Corner
    4  POINT (-70.25659 43.68287)     East Deering
    5  POINT (-70.24676 43.66755)         East End
    6  POINT (-70.29654 43.67430)   Deering Center
    7  POINT (-70.27895 43.67926)        Back Cove
    8  POINT (-70.25539 43.66633)     East Bayside
    9  POINT (-70.26962 43.65628)         Parkside

"""
from collections.abc import Iterable
import io
import json
import os
import time
import datetime
import uuid
from enum import Enum
from copy import deepcopy

import numpy as np
import requests
import pandas as pd

import warnings

from .. import discover as disco
from descarteslabs.auth import Auth
from descarteslabs.config import get_settings
from descarteslabs.exceptions import BadRequestError
from ..client.services.storage import Storage
from ..common.ibis.client import api as serializer
from ..common.proto.vektorius import vektorius_pb2
from ..client.deprecation import deprecate_func
from ..discover import AssetType


def _default_json_serializer(obj):
    """
    This function will be called by json.dumps to handle
    any objects that are not JSON-serializeable by default.
    """
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    else:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class JobStatus(str, Enum):
    _s = vektorius_pb2.JobStatus
    REGISTERED = _s.Name(_s.REGISTERED)
    PENDING = _s.Name(_s.PENDING)
    RUNNING = _s.Name(_s.RUNNING)
    CANCELLED = _s.Name(_s.CANCELLED)
    SUCCESS = _s.Name(_s.SUCCESS)
    FAILURE = _s.Name(_s.FAILURE)


class Tables(object):
    def __init__(
        self,
        host=None,
        port=None,
        auth=None,
        discover_client=None,
        database=None,
        **grpc_client_kwargs,
    ):
        """
        Tables client for interacting with tabular data.

        :param str host: Service hostname.
        :param int port: Service port.
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param database: Backend database
        """
        if host is None:
            host = get_settings().tables_host
        if port is None:
            port = int(get_settings().tables_port)

        self.connection = serializer.connect(
            host=host, port=port, auth=auth, **grpc_client_kwargs
        )

        self.storage_client = Storage(auth=auth)
        self.storage_type = "tmp"  # TODO "data"?

        self.database = database

        self.auth = auth or Auth.get_default_auth()
        self.discover_client = discover_client or disco.Discover(auth=auth)

    @property
    def dialect(self):
        return self.connection.dialect

    def table(self, name, owner=None):
        """
        Retrieve a table by name

        :param str name: name of table
        :param str owner: table owner, defaults to the user's email.

        :return: table expression
        :rtype: ibis.expr.types.TableExpr
        """
        if owner is None:
            owner = self.auth.payload["email"]
        return self.connection.table(name, owner, database=self.database)

    def inspect_dataset(self, dataset):
        """
        Extract schema and crs from an existing local dataset

        :param str|dataframe dataset: tabular dataset, path to a file
            or a pandas geodataframe

        :return: Tuple; schema, srid
        :rtype: (mapping, int)
        """

        try:
            from geopandas.array import GeometryDtype
            import fiona
        except ImportError:
            raise ImportError(
                "Failed to import geospatial libraries, please install geopandas and fiona."
            )

        try:
            # handle pandas geodataframes
            geomtype = dataset.geom_type.unique()[0]

            # pop geometry off the list
            prop_dtypes = dict(
                [
                    (col, dtype)
                    for col, dtype in zip(dataset.columns, dataset.dtypes.to_list())
                    if not isinstance(dtype, GeometryDtype)
                ]
            )

            dtypes_to_vek = {
                np.dtype("O"): "text",
                np.dtype("int64"): "int",
                np.dtype("float64"): "float",
                np.dtype("<M8[ns]"): "datetime",
            }

            schema = {
                "properties": dict(
                    (k, dtypes_to_vek.get(v, "text")) for k, v in prop_dtypes.items()
                ),
                "geometry": geomtype,
            }
            crs = dataset.crs

        except AttributeError:
            # handle spatial datasets
            if dataset.endswith(".zip"):
                # fiona requires a zip uri
                dataset = f"zip://{dataset}"
            with fiona.open(dataset, mode="r") as fc:
                schema = fc.schema
                crs = fc.crs

            new_properties = dict(
                (k, v.split(":")[0]) for k, v in schema["properties"].items()
            )
            schema["properties"] = new_properties

        if crs and "init" in crs and crs["init"].lower().startswith("epsg"):
            srid = int(crs["init"].replace("epsg:", ""))
        else:
            srid = 0

        return schema, srid

    def create_table(self, table_name, schema, srid, primary_key=None):
        """
        Create a table

        :param str table_name: name of table. Can only contain alphanumeric and "_" characters.
        :param dict schema: schema mapping
        :param int srid: spatial reference identifier
        :param str|list primary_key: column name(s) to use as primary key. If None, a default key is used.

        :return: table name
        :rtype: str
        """

        DEFAULT_PK = "auto_id"

        # Copy off the schema into a variable that is internal to the client so
        # that we don't alter the user's schema variable, which very likely has
        # the same variable name
        _internal_schema = deepcopy(schema)

        if not primary_key:
            warnings.warn(
                f'Primary key not provided, adding a new "{DEFAULT_PK}" column as an auto-incrementing primary key.',
                UserWarning,
            )
            if DEFAULT_PK not in _internal_schema["properties"]:
                _internal_schema["properties"][DEFAULT_PK] = "auto"
                primary_key = DEFAULT_PK
            else:
                raise BadRequestError(
                    f'Primary key "{DEFAULT_PK}" already exists in schema; specify primary key manually.'
                )

        if not isinstance(primary_key, list):
            primary_key = [primary_key]

        # if primary key/s is/are not present in the schema then there's no point
        # in continuing
        for pk in primary_key:
            if pk not in _internal_schema["properties"]:
                raise BadRequestError(f'Could not find primary key "{pk}" in schema.')

        # TODO can we avoid reaching into the `ibis` serializer
        # just to access the vektorius gRPC client method?
        response = self.connection.client._client.api["CreateTable"](
            vektorius_pb2.CreateTableRequest(
                table_name=table_name,
                properties_schema=_internal_schema["properties"],
                geometry_type=_internal_schema["geometry"],
                srid=srid,
                primary_key=primary_key,
            )
        )
        if response.status is not True:
            raise BadRequestError(
                f"Could not create {table_name} because of {response.message}"
            )
        else:
            return table_name

    def list_tables(self, name: str = None):
        """
        Return a list of tables

        :param str name: tables matching the search string will be included.
            Wildcards * and ? can be used to match any number of any character or a
            single character respectively.

        :return: list of tables owned by the authenticated user organized by schema
        :rtype: Dict(str, Dict(str, List(str)))
        """
        owner_tables = []
        editor_tables = {}
        viewer_tables = {}

        # retrieve a list of tables associated with this user
        assets = self.discover_client.list_assets(
            filters={"type": AssetType.TABLE, "name": name}
        )
        for asset in assets:
            # sym_links have a slightly different structure from actual vector
            # we need to get the display name and asset name from those as well
            if asset._type() == AssetType.TABLE:
                display_name = asset.display_name
                asset_name = asset.asset_name
            elif asset._type() == AssetType.SYM_LINK:
                display_name = asset.sym_link.target_asset_display_name
                asset_name = asset.sym_link.target_asset_name
            else:
                # logically this shouldn't be reached but if it is
                # there's an error with the discover client
                continue

            grants = self.discover_client.list_access_grants(asset_name)
            for grant in grants:
                # figure out the asset owner
                owner_id = next(
                    (g for g in grants if "owner" in g.access), None
                ).target_id

                if grant.target_id == self.auth.payload["email"]:
                    perm = grant.access.split("/")[2]
                    # note that we do NOT display owner_id for owner tables
                    if perm == "owner":
                        owner_tables.append(display_name)
                    elif perm == "editor":
                        if owner_id not in editor_tables:
                            editor_tables[owner_id] = []
                        editor_tables[owner_id].append(display_name)
                    elif perm == "viewer":
                        if owner_id not in viewer_tables:
                            viewer_tables[owner_id] = []
                        viewer_tables[owner_id].append(display_name)
                    else:
                        # this would indicate an issue with acltraz
                        # TODO: better error handling here
                        continue
                else:
                    # list_access_grants() gives us all of the grants
                    # associated with an asset but we only care about grants
                    # pertaining to the user
                    continue

        tables = {
            "owner": owner_tables,
            "editor": editor_tables,
            "viewer": viewer_tables,
        }
        return tables

    def delete_table(self, table_name):
        """
        Delete a table by name

        :param str table_name: name of table to delete
        """
        response = self.connection.client._client.api["DeleteTable"](
            vektorius_pb2.DeleteTableRequest(
                table_name=table_name,
            )
        )
        if response.status is not True:
            raise BadRequestError(
                f"Could not delete {table_name} because of {response.message}"
            )
        else:
            return table_name

    def _normalize_features(self, obj):
        """
        :param object obj: Python object representing GeoJSON-like features.
            This can be an object with __geo_interface__ method (e.g. GeoDataFrame),
            a GeoJSON-like FeatureCollection mapping, or
            a single GeoJSON-like Feature mapping, or
            an iterable of GeoJSON-like Feature mappings

        :return: Iterable of GeoJSON-like Feature mappings
        """
        if hasattr(obj, "__geo_interface__"):
            features = obj.__geo_interface__["features"]
        elif isinstance(obj, pd.DataFrame):
            features = (
                {"type": "Feature", "properties": r, "geometry": None}
                for r in obj.to_dict("records")
            )
        elif "features" in obj:
            features = obj["features"]
        elif "properties" in obj and "geometry" in obj:
            features = [obj]
        elif isinstance(obj, Iterable):
            # TODO we have to trust that the contents are GeoJSON-like features
            # to avoid consuming any stateful iterators
            features = obj
        else:
            raise BadRequestError("Could not find any GeoJSON-like features")

        yield from features

    def _encode_feature_line(self, feature):
        """
        Encode a geojson-like feature mapping into a single line of utf-8 bytes
        """
        if not ("properties" in feature and "geometry" in feature):
            raise BadRequestError("Not a valid GeoJSON feature")

        # special case: we know we won't use the optional bbox element on ingest
        # so we can delete it here to save bytes
        try:
            del feature["bbox"]
        except KeyError:
            pass
        # Likewise, we have no need for the top-level 'id' field
        # since the primary key lives in properties
        try:
            del feature["id"]
        except KeyError:
            pass

        return (
            json.dumps(feature, default=_default_json_serializer).encode("utf-8")
            + b"\n"
        )

    def upload_file(self, file_obj, table_name, owner=None, file_ext=None):
        """
        Upload an file to an existing table.

        :param str|file-like file_obj: File-like object or name of file which
            will be uploaded. If this is a file-like object it must use byte mode (for
            example, a file opened in binary mode such as with ``open(filename, 'rb')``).
        :param str table_name: name of table
        :param str owner: table owner, defaults to the user's email.
        :param str file_ext: file extension to use, including the leading '.'.
            Required for a file-like file_obj. Ignored if file_obj is a string.

        :return: Job identifier
        :rtype: str
        """

        if owner is None:
            owner = self.auth.payload["email"]

        # Determine key name; add a timestamp prefix to ensure uniqueness
        if isinstance(file_obj, str):
            key = os.path.basename(file_obj)
        else:
            if file_ext is None:
                raise BadRequestError("file_ext is required")
            key = f"upload{file_ext}"
        timestamp = time.strftime("%Y%m%d-%H%M%s")
        key = f"{timestamp}-{key}"

        # Get a signed URL from the storage service
        url = self.storage_client.get_upload_url(key, self.storage_type)

        # Why not validate file contents first to ensure it's a valid source?
        # Only possible with a file path since a file object is stateful
        # Upload the file to storage
        if isinstance(file_obj, str):
            with open(file_obj, "rb") as fh:
                r = requests.put(url, data=fh)
                r.raise_for_status()
        else:
            r = requests.put(url, data=file_obj)
            r.raise_for_status()

        response = self.connection.client._client.api["CreateIngestJob"](
            vektorius_pb2.CreateIngestJobRequest(
                key=key,
                storage_type=self.storage_type,
                table_name=table_name,
                owner=owner,
            )
        )
        return response.job_id

    def insert_rows(self, obj, table_name, owner=None):
        """
        Add rows to an existing table

        :param object obj: Python object representing GeoJSON-like features.
            This can be an object with __geo_interface__ method (e.g. GeoDataFrame),
            a GeoJSON-like FeatureCollection mapping, or
            a single GeoJSON-like Feature mapping, or
            an iterable of GeoJSON-like Feature mappings
        :param str table_name: name of vector table
        :param str owner: table owner, defaults to the user's email.

        :return: Job identifier
        :rtype: str
        """
        # Determine key name; add a timestamp prefix to ensure uniqueness
        # note file extension; .ndgeojson is handled differently on ingest
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        key = f"{timestamp}-{uuid.uuid4()}-features.geojsonl"

        if owner is None:
            owner = self.auth.payload["email"]
        # Get a signed URL from the storage service
        url = self.storage_client.get_upload_url(key, self.storage_type)

        # If the object is a file path, use upload_file instead
        try:
            if os.path.exists(obj):
                raise BadRequestError(
                    "Input looks like a file path; use upload_file method instead"
                )
        except TypeError:
            pass

        features = self._normalize_features(obj)

        # Construct a newline-deliminted JSON document in memory
        # TODO this can likely be done in a streaming fashion
        file_obj = io.BytesIO()
        file_obj.writelines(self._encode_feature_line(f) for f in features)
        file_obj.seek(0)

        r = requests.put(url, data=file_obj)
        r.raise_for_status()

        response = self.connection.client._client.api["CreateIngestJob"](
            vektorius_pb2.CreateIngestJobRequest(
                key=key,
                storage_type=self.storage_type,
                table_name=table_name,
                owner=owner,
            )
        )
        return response.job_id

    def delete_rows(self, ids, table_name, pk_order=None, owner=None):
        """
        Deletes rows from an existing table

        :param list, pd.Series: Iterable of ids to delete. These ids must match
            the primary key or keys of the table. Ids may be either elements
            by themselves or list/tuples of elements in the case of composite primary keys.
        :param str table_name: name of table
        :param str owner: table owner, defaults to the user's email.
        :param list pk_order: Required when deleting rows from tables with
            composite primary keys. Specify the column order for each element in ids.
        :return: Job identifier
        :rtype: str
        """
        if owner is None:
            owner = self.auth.payload["email"]

        if isinstance(ids, pd.Series):
            ids = ids.tolist()

        def json_hander(obj):
            # custom handler for special objects since date, datetime objects
            # can be primary keys
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

        # try jsoning ids
        ids_string = json.dumps(ids, default=json_hander)

        response = self.connection.client._client.api["CreateDeleteRowsJob"](
            vektorius_pb2.CreateDeleteRowsJobRequest(
                ids=ids_string,
                table_name=table_name,
                pk_order=pk_order,
                owner=owner,
            )
        )
        return response.job_id

    @deprecate_func(
        "insert_features has been deprecated. Please use insert_rows instead."
    )
    def insert_features(self, obj, table_name, owner=None):
        """
        :param object obj: Python object representing GeoJSON-like features.
            This can be an object with __geo_interface__ method (e.g. GeoDataFrame),
            a GeoJSON-like FeatureCollection mapping, or
            a single GeoJSON-like Feature mapping, or
            an iterable of GeoJSON-like Feature mappings
        :param str table_name: name of vector table
        :param str owner: table owner, defaults to the user's email.

        :return: Job identifier
        :rtype: str
        """
        return self.insert_rows(obj, table_name, owner)

    def check_status(self, jobid):
        """
        Returns the status of a job

        :param int jobid: Job identifier

        :return: Tuple; completed, message
        :rtype: (JobStatus, str)
        """
        response = self.connection.client._client.api["GetJobStatus"](
            vektorius_pb2.JobStatusRequest(job_id=jobid)
        )

        status = vektorius_pb2.JobStatus.Name(response.status)
        return JobStatus(status), response.message

    def wait_until_completion(self, jobid, raise_on_failure=True, poll_interval=5.0):
        """
        Returns the status of the job, blocking until the job is completed

        :param int jobid: Job identifier
        :param bool raise_on_failure: Raise an exception if the service reports
          an asyncronous failure.
        :param float poll_interval: Pause between status checks, seconds

        :return: Tuple; completed, message
        :rtype: (JobStatus, str)

        :raises RuntimeError: In case the service reports an asynchronous failure
        and the `raise_on_failure` is set to ``True``.
        """
        status_done = [JobStatus.SUCCESS, JobStatus.CANCELLED, JobStatus.FAILURE]

        completed = False
        while not completed:
            (status, message) = self.check_status(jobid)
            if status in status_done:
                completed = True
                break
            time.sleep(poll_interval)

        status_failed = [JobStatus.CANCELLED, JobStatus.FAILURE]
        if status in status_failed:
            if raise_on_failure:
                raise RuntimeError(f"{status.value} {message}")

        return (status, message)
