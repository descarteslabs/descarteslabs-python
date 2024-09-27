# Copyright 2018-2024 Descartes Labs.

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd
import shapely

from descarteslabs.exceptions import NotFoundError

from ..common.geo import GeoContext
from ..common.property_filtering import Properties
from ..common.vector.models import GenericFeatureBaseModel, VectorBaseModel

# To avoid confusion we import these as <module>_<function>
from .features import Statistic
from .features import add as features_add
from .features import aggregate as features_aggregate
from .features import delete as features_delete
from .features import get as features_get
from .features import join as features_join
from .features import query as features_query
from .features import sjoin as features_sjoin
from .features import update as features_update
from .products import create as products_create
from .products import delete as products_delete
from .products import get as products_get
from .products import list as products_list
from .products import update as products_update
from .vector_client import VectorClient

ACCEPTED_GEOM_TYPES = [
    "Point",
    "MultiPoint",
    "Line",
    "LineString",
    "MultiLine",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
    "GeometryCollection",
]


# Supporting functions for geometry filtering.


def _geojson_to_shape(gj: dict) -> shapely.geometry.base.BaseGeometry:
    """
    Convert a GeoJSON dict into a shapely shape.

    Parameters
    ----------
    gj: dict
        GeoJSON object

    Returns
    -------
    shp: shapely.geometry.base.BaseGeometry
        Shapely shape for the geojson.
    """
    return shapely.geometry.shape(gj)


def _dl_aoi_to_shape(aoi: GeoContext) -> shapely.geometry.base.BaseGeometry:
    """
    Convert an AOI object into a shapely shape.

    Parameters
    ----------
    aoi: descarteslabs.geo.GeoContext
        AOI for which we want a shapely shape.

    Returns
    -------
    shp: shapely.geometry.base.BaseGeometry
        Shapely shape for this AOI.
    """
    # This only works if we have a geometry or the aoi.bounds_crs is 4326!
    # We have no way in the client to do the right thing otherwise.
    if aoi.geometry is not None:
        return aoi.geometry
    if aoi.bounds_crs == "EPSG:4326":
        return shapely.geometry.box(*list(aoi.bounds))
    raise ValueError(
        "Can't convert aoi to shape. Please provide aoi.geometry or aoi.bounds_crs='EPSG:4326'"
    )


def _to_shape(
    aoi: Optional[Union[GeoContext, dict, shapely.geometry.base.BaseGeometry]] = None
) -> Union[shapely.geometry.base.BaseGeometry, None]:
    """
    Attempt to convert input to a shapely object.

    Raise an exception for non-None values that can't be converted.

    Parameters
    ----------
    aoi: Optional[Union[descarteslabs.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
        Optional AOI to convert to a shapely object.

    Returns
    -------
    shp: Union[shapely.geometry.base.BaseGeometry, None]
        None if aoi is None, or a shapely representation of the aoi.
    """

    if not aoi:
        return None

    # Convert the AOI object to a shapely object so we can
    # perform intersections.
    if isinstance(aoi, dict):
        aoi = _geojson_to_shape(aoi)
    elif issubclass(type(aoi), GeoContext):
        aoi = _dl_aoi_to_shape(aoi)
    elif issubclass(type(aoi), shapely.geometry.base.BaseGeometry):
        return aoi
    else:
        raise TypeError(f"'{aoi}' not recognized as an aoi")

    return aoi


def _shape_to_geojson(shp: shapely.geometry.base.BaseGeometry) -> dict:
    """
    Convert a shapely object into a GeoJSON.

    Parameters
    ----------
    shp: shapely.geometry.base.BaseGeometry

    Returns
    -------
    gj: dict
        GeoJSON dict for this shape
    """
    if shp:
        return shapely.geometry.mapping(shp)
    return None


class TableOptions:
    """
    A class for controlling Table options and parameters.
    """

    def __init__(
        self,
        product_id: str,
        aoi: Optional[
            Union[GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
        property_filter: Optional[Properties] = None,
        columns: Optional[List[str]] = None,
    ):
        """
        Initialize a TableOptions instance.

        Parameters
        ----------
        product_id: str
            Product ID of a Vector Table.
        aoi: Optional[Union[descarteslabs.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
            AOI to associate with this TableOptions.
        property_filter: Optional[Properties]
            Property filter to associate with this TableOptions.
        columns: Optional[List[str]]
            List of columns to include with this TableOptions.
        """
        self._product_id = product_id
        self._aoi = _to_shape(aoi)
        self._property_filter = property_filter
        self._columns = columns

    @property
    def product_id(self) -> str:
        """
        Return the product ID of this TableOptions.

        Parameters
        ----------
        None

        Returns
        -------
        str
        """
        return self._product_id

    @product_id.setter
    def product_id(self, product_id: str) -> None:
        """
        Set the product ID of this TableOptions.

        Parameters
        ----------
        product_id: str
            Product ID of a Vector Table.

        Returns
        -------
        None
        """
        if not isinstance(product_id, str):
            raise TypeError("'product_id' must be of type <str>")
        self._product_id = product_id

    @property
    def aoi(self) -> shapely.geometry.shape:
        """
        Return the AOI option of this TableOptions.

        Parameters
        ----------
        None

        Returns
        -------
        shapely.geometry.shape
        """
        return self._aoi

    @aoi.setter
    def aoi(
        self,
        aoi: Optional[
            Union[GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
    ) -> None:
        """
        Set the AOI option of this TableOptions.

        Parameters
        ----------
        aoi: Union[descarteslabs.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
            AOI of this TableOptions.

        Returns
        -------
        None
        """
        self._aoi = _to_shape(aoi)

    @property
    def property_filter(self) -> Properties:
        """
        Return the property_filter option of this TableOptions.

        Parameters
        ----------
        None

        Returns
        -------
        Properties
        """
        return self._property_filter

    @property_filter.setter
    def property_filter(self, property_filter: Optional[Properties] = None) -> None:
        """
        Set the property_filter option of this TableOptions.

        Parameters
        ----------
        property_filter: Properties
            property_filter option of this TableOptions.

        Returns
        -------
        None
        """
        if hasattr(property_filter, "jsonapi_serialize"):
            self._property_filter = property_filter
        elif not property_filter:
            self._property_filter = None
        else:
            raise TypeError("'property_filter' must be of type <None> or <Properties>")

    @property
    def columns(self) -> List[str]:
        """
        Return the columns option of this TableOptions.

        Parameters
        ----------
        None

        Returns
        -------
        list
        """
        return self._columns

    @columns.setter
    def columns(self, columns: Optional[List[str]] = None) -> None:
        """
        Set the columns option of this TableOptions.

        Parameters
        ----------
        columns: List[str]
            List of columns to include.

        Returns
        -------
        None
        """
        if isinstance(columns, list):
            self._columns = columns
        elif not columns:
            self._columns = None
        else:
            raise TypeError("'columns' must be of type <None> or <list>")
        self._columns = columns


class Table:
    """
    A class for creating and interacting with Vector Tables.
    """

    def __init__(
        self,
        table_parameters: Union[dict, str],
        options: TableOptions = None,
        client: Optional[VectorClient] = None,
    ):
        """
        Initialize a Vector Table instance.

        Users should create a Table instance via `Table.get` or `Table.create`.

        Parameters
        ----------
        product_parameters: Union[dict, str]
            Dictionary of product parameters or the product ID of a Vector Table.
        client : VectorClient, optional
            Client to use for requests. If not provided, the default client will be used.
        """

        if client is None:
            client = VectorClient.get_default_client()

        if isinstance(table_parameters, str):
            table_parameters = products_get(table_parameters, client=client)

        for k, v in table_parameters.items():
            setattr(self, f"_{k}", v)

        if not options:
            options = TableOptions(self.id)

        if not isinstance(options, TableOptions):
            raise TypeError(("'options' must be of type <TableOptions>"))

        self.options = options
        self._client = client

    @staticmethod
    def get(
        product_id: str,
        aoi: Optional[
            Union[GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
        property_filter: Optional[Properties] = None,
        columns: Optional[List[str]] = [],  # noqa: M511
        client: Optional[VectorClient] = None,
    ) -> Table:
        """
        Get a Vector Table instance from a Vector Table product ID. Raise an exception if
        this `product_id` doesn't exit.

        Parameters
        ----------
        product_id: str
            Product ID of the Vector Table.
        aoi: Optional[Union[descarteslabs.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
            AOI to associate with this Vector Table.
        property_filter: Optional[Properties]
            Property filter to associate with this Vector Table.
        columns: Optional[List[str]]
            List of columns to include.
        client : VectorClient, optional
            Client to use for requests. If not provided, the default client will be used.

        Returns
        -------
        Table
        """
        options = TableOptions(
            product_id=product_id,
            aoi=aoi,
            property_filter=property_filter,
            columns=columns,
        )

        if client is None:
            client = VectorClient.get_default_client()

        return Table(
            table_parameters=products_get(product_id, client=client),
            options=options,
            client=client,
        )

    @staticmethod
    def create(
        product_id: str,
        name: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        readers: Optional[List[str]] = None,
        writers: Optional[List[str]] = None,
        owners: Optional[List[str]] = None,
        model: Optional[VectorBaseModel] = GenericFeatureBaseModel,
        client: Optional[VectorClient] = None,
    ) -> Table:
        """
        Create a Vector Table.

        Parameters
        ----------
        product_id : str
            Product ID of the Vector Table.
        name : str
            Name of the Vector Table.
        description : str, optional
            Description of the Vector Table.
        tags : list of str, optional
            A list of tags to associate with the Vector Table.
        readers : list of str, optional
            A list of Vector Table readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        writers : list of str, optional
            A list of Vector Table writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        owners : list of str, optional
            A list of Vector Table owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        model : VectorBaseModel, optional
            A model that provides a user provided schema for the Vector Table.
        client : VectorClient, optional
            Client to use for requests. If not provided, the default client will be used.

        Returns
        -------
        Table
        """

        if client is None:
            client = VectorClient.get_default_client()

        return Table(
            table_parameters=products_create(
                product_id=product_id,
                name=name,
                description=description,
                tags=tags,
                readers=readers,
                writers=writers,
                owners=owners,
                model=model,
                client=client,
            ),
            client=client,
        )

    @staticmethod
    def list(
        tags: Optional[List[str]] = None,
        client: Optional[VectorClient] = None,
    ) -> List[Table]:
        """
        List available Vector Tables.

        Parameters
        ----------
        tags: list of str
            Optional list of tags a Vector Table must have to be returned.
        client : VectorClient, optional
            Client to use for requests. If not provided, the default client will be used.

        Returns
        -------
        List[Table]
        """

        if client is None:
            client = VectorClient.get_default_client()

        return [
            Table(table_parameters=d, client=client)
            for d in products_list(tags=tags, client=client)
        ]

    @property
    def id(self) -> str:
        """
        Return the product ID of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        str
        """
        return self._id

    @property
    def created(self) -> datetime:
        """
        Return the datetime this Vector Table was created.

        Parameters
        ----------
        None

        Returns
        -------
        datetime
        """
        try:
            return datetime.fromisoformat(self._created)
        except ValueError:
            return datetime.strptime(self._created, "%Y-%m-%dT%H:%M:%S.%f")

    @property
    def is_spatial(self) -> bool:
        """
        Return a boolean indicating whether or not this Vector Table is spatial.

        Parameters
        ----------
        None

        Returns
        -------
        bool
        """
        return self._is_spatial

    @property
    def name(self) -> str:
        """
        Return the name of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        str
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the name of this Vector Table.

        Parameters
        ----------
        value: str
            Name of the Vector Table.

        Returns
        -------
        None
        """
        if isinstance(value, str):
            self._name = value
        else:
            raise TypeError("Table 'name' must be of type <str>")

    @property
    def description(self) -> str:
        """
        Return the description of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        str
        """
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        """
        Set the description of this Vector Table.

        Parameters
        ----------
        value: str
            Description of the Vector Table.

        Returns
        -------
        None
        """
        if isinstance(value, str):
            self._description = value
        else:
            raise TypeError("Table 'description' must be of type <str>")

    @property
    def tags(self) -> List[str]:
        """
        Return the tags of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        List[str]
        """
        return self._tags

    @tags.setter
    def tags(self, value: List[str]) -> None:
        """
        Set the tags for this Vector Table.

        Parameters
        ----------
        value: List[str]
             A list of tags to associate with the Vector Table.
        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._tags = value
        else:
            raise TypeError("Table 'tags' must be of type <list>")

    @property
    def readers(self) -> List[str]:
        """
        Return the readers of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        List[str]
        """
        return self._readers

    @readers.setter
    def readers(self, value: List[str]) -> None:
        """
        Set the readers for this Vector Table.

        Parameters
        ----------
        value: List[str]
            Readers for this Vector Table.

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._readers = value
        else:
            raise TypeError("Table 'readers' must be of type <list>")

    @property
    def writers(self) -> List[str]:
        """
        Return the writers of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        List[str]
        """
        return self._writers

    @writers.setter
    def writers(self, value: List[str]) -> None:
        """
        Set the writers for the Vector Table.

        Parameters
        ----------
        value: List[str]
             Writers for the Vector Table

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._writers = value
        else:
            raise TypeError("Table 'writers' must be of type <list>")

    @property
    def owners(self) -> List[str]:
        """
        Return the owners of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        List[str]
        """
        return self._owners

    @owners.setter
    def owners(self, value: List[str]) -> None:
        """
        Set the owners for this Vector Table.

        Parameters
        ----------
        value: List[str]
            Owners of this Vector Table.

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._owners = value
        else:
            raise TypeError("Table 'owners' must be of type <list>")

    @property
    def model(self) -> dict:
        """
        Return the model of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        dict
        """
        return self._model

    @property
    def columns(self) -> List[str]:
        """
        Return the column names of this Vector Table.

        Parameters
        ----------
        None

        Returns
        -------
        List[str]
        """
        return list(self._model["properties"].keys())

    @property
    def parameters(self) -> dict:
        """
        Return the Vector Table parameters as dictionary.

        Parameters
        ----------
        None

        Returns
        -------
        dict
        """

        keys = [
            "_id",
            "_name",
            "_description",
            "_tags",
            "_readers",
            "_writers",
            "_owners",
            "_model",
        ]

        params = {}

        for k in keys:
            params[k.lstrip("_")] = self.__dict__[k]

        return params

    def __repr__(self) -> str:
        """
        Generate a string representation of this Vector Table.

        Parameters
        ----------
        None

        Return
        ------
        str
        """
        return f"Table: {self.name}\n  id: {self.id}\n  created: {self.created.strftime('%a %b %d %H:%M:%S %Y')}"

    def __str__(self) -> str:
        """
        Generate a string representation of this Vector Table.

        Parameters
        ----------
        None

        Return
        ------
        str
        """
        return self.__repr__()

    def save(self) -> None:
        """
        Save/update this Vector Table.

        Parameters
        ----------
        None

        Return
        ------
        None
        """
        products_update(
            product_id=self.id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            readers=self.readers,
            writers=self.writers,
            owners=self.owners,
            client=self._client,
        )

    def add(
        self,
        dataframe: Union[pd.DataFrame, gpd.GeoDataFrame],
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Add a dataframe to this table. If the Vector Table has a `geometry` column
        the dataframe must be a GeoPandas GeoDataFrame, otherwise a Pandas DataFrame
        must be provided. Note that the returned dataframe will have UUID attribution
        for each row.

        Parameters
        ----------
        dataframe:gpd.GeoDataFrame|pd.DataFrame
            GeoPandas dataframe to add to this table.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame]
        """

        return features_add(
            product_id=self.id,
            dataframe=dataframe,
            client=self._client,
        )

    def get_feature(
        self,
        feature_id: str,
    ) -> Feature:
        """
        Get a Vector Feature from this Vector Table instance.

        Parameters
        ----------
        feature_id: str
            Vector Feature ID for the feature to get.

        Returns
        -------
        Feature
        """
        return Feature.get(id=f"{self.id}:{feature_id}", client=self._client)

    def try_get_feature(self, feature_id: str) -> Feature:
        """
        Get a Vector Feature from this Vector Table instance.

        Parameters
        ----------
        feature_id: str
            Vector Feature ID for the feature to get.

        Returns
        -------
        Feature
        """
        try:
            return Feature.get(id=f"{self.id}:{feature_id}", client=self._client)
        except NotFoundError:
            return None

    def visualize(
        self,
        name: str,
        map,  # No type hint because we don't want to require ipyleaflet
        vector_tile_layer_styles: Optional[dict] = None,
        override_options: TableOptions = None,
    ) -> None:
        """
        Visualize this Vector Table as an `ipyleaflet` VectorTileLayer.
        The property_filter and the columns specified with the Table
        options will be honored but the AOI option will be ignored.

        To use this method, you must have the `ipyleaflet` package installed,
        it is not installed by default when installing the Descartes Labs python
        client.

        Parameters
        ----------
        name : str
            Name to give to the ipyleaflet VectorTileLayer.
        map: ipyleaflet.leaflet.Map
            Map to which to add the layer
        vector_tile_layer_styles : dict, optional
            Vector tile styles to apply. See https://ipyleaflet.readthedocs.io/en/latest/layers/vector_tile.html for
            more details.
        override_options: TableOptions
            Override options for this query. AOI option is ignored
            when invoking this method.

        Returns
        -------
        DLVectorTileLayer
            Vector tile layer that can be added to an ipyleaflet map.
        """
        # Avoid circular import
        from .tiles import create_layer

        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'options' must be of type <TableOptions>")

        lyr = create_layer(
            product_id=self.id,
            name=name,
            property_filter=options.property_filter,
            columns=options.columns,
            vector_tile_layer_styles={self.id: vector_tile_layer_styles},
        )
        for layer in map.layers:
            if layer.name == name:
                map.remove_layer(layer)
                break
        map.add_layer(lyr)

    def collect(
        self, override_options: TableOptions = None
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Method to execute a query/collect on this Vector Table, returning a
        dataframe. Table options will be honored when executing the query.
        If the Vector Table has a `geometry` column and the `geometry` column
        is included in the Table options, a GeoPandas GeoDataFrame will be
        returned, otherwise a Pandas DataFrame will be returned.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame]
        """

        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'options' must be of type <TableOptions>")

        return features_query(
            options.product_id,
            property_filter=options.property_filter,
            aoi=_shape_to_geojson(options.aoi),
            columns=options.columns,
            client=self._client,
        )

    def join(
        self,
        join_table: [Union[Table, TableOptions]],
        join_type: Literal["INNER", "LEFT", "RIGHT"],
        join_columns: List[Tuple[str, str]],
        override_options: Optional[TableOptions] = None,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Method to execute a relational join between two Vector Tables,
        returning a dataframe. Table options will be honored when executing
        the query. If either Vector Table has a `geometry` column and either
        Vector Table included the 'geometry' column in the Table options, a
        GeoPandas GeoDataFrame will be returned, otherwise a Pandas DataFrame
        will be returned.

        Parameters
        ----------
        join_table: [Union[Table, TableOptions]]
            The Vector Table or TableOptions to join.
        join_type: Literal["INNER", "LEFT", "RIGHT"]
            The type of join to perform. Must be one of INNER,
            LEFT, or RIGHT.
        join_columns: List[Tuple[str, str]]
            List of column names to join on. Must be formatted
            as [(table1_col1, table2_col2), ...].
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame]
        """
        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'override_options' must be of type <TableOptions>")

        if isinstance(join_table, TableOptions):
            pass
        elif isinstance(join_table, Table):
            join_table = join_table.options
        else:
            raise TypeError("'join_table' must be of type <TableOptions>")

        include_columns = [tuple(options.columns), tuple(join_table.columns)]

        return features_join(
            input_product_id=options.product_id,
            join_product_id=join_table.product_id,
            join_type=join_type,
            join_columns=join_columns,
            include_columns=include_columns,
            input_property_filter=options.property_filter,
            input_aoi=_shape_to_geojson(options.aoi),
            join_property_filter=join_table.property_filter,
            join_aoi=_shape_to_geojson(join_table.aoi),
            client=self._client,
        )

    def sjoin(
        self,
        join_table: [Union[Table, TableOptions]],
        join_type: Literal["INTERSECTS", "CONTAINS", "OVERLAPS", "WITHIN"],
        override_options: Optional[TableOptions] = None,
        keep_all_input_rows: Optional[bool] = False,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Method to execute a spatial join between two Vector Tables,
        returning a dataframe. Table options will be honored when executing
        the query. Both Vector Tables must have a `geometry` column. If either
        Vector Table included the 'geometry' column in the Table options, a
        GeoPandas GeoDataFrame will be returned, otherwise a Pandas DataFrame
        will be returned.

        Parameters
        ----------
        join_table: [Union[Table, TableOptions]]
            The Vector Table or TableOptions to join.
        join_type: Literal["INTERSECTS", "CONTAINS", "OVERLAPS", "WITHIN"]
            The type of join to perform. Must be one of INTERSECTS,
            CONTAINS, OVERLAPS, WITHIN.
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame]
        """
        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'override_options' must be of type <TableOptions>")

        if isinstance(join_table, TableOptions):
            join_is_spatial = Table.get(product_id=join_table.product_id).is_spatial
        elif isinstance(join_table, Table):
            join_is_spatial = join_table.is_spatial
            join_table = join_table.options
        else:
            raise TypeError("'join_table' must be of type <TableOptions>")

        if not self.is_spatial and not join_is_spatial:
            raise TypeError("Both Tables must have a geometry column for spatial joins")

        include_columns = [tuple(options.columns), tuple(join_table.columns)]

        return features_sjoin(
            input_product_id=options.product_id,
            join_product_id=join_table.product_id,
            join_type=join_type,
            include_columns=include_columns,
            input_property_filter=options.property_filter,
            input_aoi=_shape_to_geojson(options.aoi),
            join_property_filter=join_table.property_filter,
            join_aoi=_shape_to_geojson(join_table.aoi),
            keep_all_input_rows=keep_all_input_rows,
            client=self._client,
        )

    def _aggregate(
        self, statistic: Statistic, override_options: TableOptions
    ) -> Union[int, dict]:
        """
        Private method for handling aggregate functions. The statistic
        COUNT will always return an integer. All other statistics will
        return a dictionary of results. Keys of the dictionary will be
        the column names requested appended with the statistic
        ('column_1.STATISTIC') and values are the result of the aggregate
        statistic.

        Parameters
        ----------
        statistic: Statistic
            Statistic to calculate.
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        Union[int, dict]
        """
        options = override_options if override_options else self.options

        if not isinstance(statistic, Statistic):
            raise TypeError("'statistic' must be of type <Statistic>")

        if not isinstance(options, TableOptions):
            raise TypeError("'options' must be of type <TableOptions>")

        return features_aggregate(
            product_id=options.product_id,
            statistic=statistic,
            columns=options.columns,
            property_filter=options.property_filter,
            aoi=_shape_to_geojson(options.aoi),
            client=self._client,
        )

    def count(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> int:
        """
        Method to return the row count of a Vector Table. Table options
        will be honored when counting rows.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        int
        """

        return self._aggregate(
            statistic=Statistic.COUNT, override_options=override_options
        )

    def sum(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to calculate the column sum for this Vector Table.
        Table options will be honored when calculating the sum. The keys
        of the returned dictionary correspond to the columns requested,
        appended with the statistic ('column_1.SUM') and the values
        are the result of the aggregate statistic.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict
        """

        return self._aggregate(
            statistic=Statistic.SUM, override_options=override_options
        )

    def min(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to calculate the column minumum for this Vector Table.
        Table options will be honored when calculating the min. The keys
        of the returned dictionary correspond to the columns requested,
        appended with the statistic ('column_1.MIN') and the values
        are the result of the aggregate statistic.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict
        """

        return self._aggregate(
            statistic=Statistic.MIN, override_options=override_options
        )

    def max(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to calculate the column maximum for this Vector Table.
        Table options will be honored when calculating the max. The keys
        of the returned dictionary correspond to the columns requested,
        appended with the statistic ('column_1.MAX') and the values
        are the result of the aggregate statistic.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict
        """

        return self._aggregate(
            statistic=Statistic.MAX, override_options=override_options
        )

    def mean(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to calculate the column mean/average for this Vector Table.
        Table options will be honored when calculating the mean. The keys
        of the returned dictionary correspond to the columns requested,
        appended with the statistic ('column_1.MEAN') and the values
        are the result of the aggregate statistic.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict
        """

        return self._aggregate(
            statistic=Statistic.MEAN, override_options=override_options
        )

    def reset_options(self) -> None:
        """
        Method to reset/clear current TableOptions.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.options.property_filter = None
        self.options.columns = None
        self.options.aoi = None

    def delete(self) -> None:
        """
        Delete this Vector Table. This method will disable all subsequent non-static method calls.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        products_delete(product_id=self.id, client=self._client)


class Feature:
    """
    A class for interacting with a Vector Feature.
    """

    def __init__(
        self,
        id: str,
        dataframe: Union[pd.DataFrame, gpd.GeoDataFrame],
        client: Optional[VectorClient] = None,
    ):
        """
        Initialize a Vector Feature instance.

        Users should create a Vector Feature instance via `Table.get_feature`
        or `Feature.get`.

        Parameters
        ----------
        id: str
            ID of the Vector Feature.
        dataframe: Union[pd.DataFrame, gpd.GeoDataFrame]
            Pandas DataFrame or a GeoPandas GeoDataFrame.
        client : VectorClient, optional
            Client to use for requests. If not provided, the default client will be used.
        """

        try:
            pid, fid = id.rsplit(":", 1)
        except ValueError:
            raise ValueError("Invalid Feature ID") from None

        if isinstance(dataframe, gpd.GeoDataFrame):
            self._is_spatial = True
        elif isinstance(dataframe, pd.DataFrame):
            self._is_spatial = False
        else:
            raise TypeError(
                "'dataframe' must be of type <pd.DataFrame> or <gpd.GeoDataFrame>"
            )
        self._id = id
        self._values = {}
        for k, v in dataframe.to_dict().items():
            self._values[k] = v[0]

        # I don't think we need to initialize this if it is None
        self._client = client

    def __repr__(self) -> str:
        """
        Generate a string representation of this Vector Feature.

        Parameters
        ----------
        None

        Return
        ------
        str
        """
        return f"Feature: {self.name}\n  id: {self.id}\n  table: {self.product_id}"

    def __str__(self) -> str:
        """
        Generate a string representation of this Vector Feature.

        Parameters
        ----------
        None

        Return
        ------
        str
        """
        return self.__repr__()

    @property
    def is_spatial(self) -> bool:
        """
        Return a boolean indicating whether or not this Vector Feature is spatial.

        Parameters
        ----------
        None

        Returns
        -------
        bool
        """
        return self._is_spatial

    @property
    def values(self) -> dict:
        """
        Return a dictionary of colum/value pairs for this Vector Feature.

        Returns
        -------
        dict
        """
        return self._values

    @values.setter
    def values(self, key, value) -> None:
        """
        Set a colum/value pair for this Vector Feature.

        Returns
        -------
        None
        """
        self._values[key] = value

    @property
    def id(self) -> str:
        """
        Return the ID of this Vector Feature.

        Returns
        -------
        str
        """
        return self._id

    @property
    def product_id(self) -> str:
        """
        Return the Vector Table product ID of this Vector Feature.

        Returns
        -------
        str
        """
        return self._id.rsplit(":", 1)[0]

    @property
    def name(self) -> str:
        """
        Return the name/uuid of ths Vector Feature.

        Returns
        -------
        str
        """
        return self._id.rsplit(":", 1)[1]

    @property
    def table(self) -> Table:
        """
        Return the Vector Table of this Vector Feature.

        Returns
        -------
        Table
        """
        return Table.get(product_id=self.product_id)

    @staticmethod
    def get(
        id: str,
        client: Optional[VectorClient] = None,
    ) -> Feature:
        """
        Get a Vector Feature instance associated with an ID.

        Parameters
        ----------
        id: str
            ID of the Vector Feature.
        client : VectorClient, optional
            Client to use for requests. If not provided, the default client will be used.

        Returns
        -------
        Feature
        """
        try:
            pid, fid = id.rsplit(":", 1)
        except ValueError:
            raise ValueError("Invalid Feature ID") from None

        if client is None:
            client = VectorClient.get_default_client()

        dataframe = features_get(product_id=pid, feature_id=fid, client=client)
        return Feature(id=id, dataframe=dataframe, client=client)

    def save(self) -> None:
        """
        Save/update this Vector Feature.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        if self.is_spatial:
            dataframe = gpd.GeoDataFrame.from_features([self], crs="EPSG:4326")
        else:
            dataframe = pd.DataFrame([self.values])
        features_update(
            product_id=self.product_id,
            feature_id=self.name,
            dataframe=dataframe,
            client=self._client,
        )

    def delete(self) -> None:
        """
        Delete this Vector Feature.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        features_delete(
            product_id=self.product_id, feature_id=self.name, client=self._client
        )

    @property
    def __geo_interface__(self) -> Union[dict, None]:
        if self.is_spatial:
            return {
                "geometry": self.values["geometry"].__geo_interface__,
                "properties": {
                    c: self.values[c] for c in self.table.columns if c != "geometry"
                },
            }
        return None
