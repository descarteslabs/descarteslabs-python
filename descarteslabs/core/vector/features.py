# © 2025 EarthDaily Analytics Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from copy import deepcopy
from enum import Enum
from io import BytesIO
from typing import List, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd

from ..client.deprecation import deprecate
from ..common.property_filtering import Properties

from .util import response_to_dataframe
from .vector_client import VectorClient


TYPES = (gpd.GeoDataFrame, pd.DataFrame)


class Statistic(str, Enum):
    """
    A class for aggregate statistics.
    """

    COUNT = "COUNT"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"


@deprecate(removed=["is_spatial"])
def add(
    product_id: str,
    dataframe: Union[gpd.GeoDataFrame, pd.DataFrame],
    is_spatial: Optional[bool] = None,
    client: Optional[VectorClient] = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Add features to a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    dataframe : Union[gpd.GeoDataFrame, pd.DataFrame]
        A GeoPandas GeoDataFrame or a Pandas DataFrame to add.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("vector.parquet", buffer, "application/octet-stream")}

    if client is None:
        client = VectorClient.get_default_client()

    # The client session normally hardwires the content type.
    # We need to remove it for this request so that the multipart
    # support in urllib3 can set it appropriately.
    session = client.session
    content_type = session.headers.get("content-type")
    if content_type is not None:
        del session.headers["content-type"]
    try:
        response = session.post(
            f"/products/{product_id}/featuresv2",
            files=files,
            timeout=(client.CONNECT_TIMEOUT, client.READ_TIMEOUT),
        )
    finally:
        if content_type is not None:
            session.headers.update({"content-type": content_type})

    return response_to_dataframe(response=response)


def query(
    product_id: str,
    property_filter: Properties = None,
    aoi: dict = None,
    columns: list = None,
    client: Optional[VectorClient] = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Query features in a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    property_filter : Properties, optional
        Property filters to filter the product with.
    aoi : dict, optional
        A GeoJSON Feature to filter the vector product with.
    columns : list, optional
        Optional list of column names.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """
    if property_filter is not None:
        property_filter = property_filter.serialize()

    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.post(
        f"/products/{product_id}/features/query",
        json={
            "format": "Parquet",
            "filter": property_filter,
            "aoi": aoi,
            "columns": columns,
        },
        timeout=(client.CONNECT_TIMEOUT, client.READ_TIMEOUT),
    )

    return response_to_dataframe(response=response)


def _join(
    params: dict,
    client: VectorClient,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Internal join function.

    Parameters
    ----------
    params : dict
        Dictionary of parameters to pass to the join endpoint.
    client : VectorClient
        Client to use for requests.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """
    params = deepcopy(params)

    input_property_filter = params.get("input_property_filter", None)
    if input_property_filter is not None:
        params["input_property_filter"] = input_property_filter.serialize()

    join_property_filter = params.get("join_property_filter", None)
    if join_property_filter is not None:
        params["join_property_filter"] = join_property_filter.serialize()

    response = client.session.post(
        "/products/features/join",
        json=params,
        timeout=(client.CONNECT_TIMEOUT, client.READ_TIMEOUT),
    )

    return response_to_dataframe(response=response)


def join(
    input_product_id: str,
    join_product_id: str,
    join_type: str,
    join_columns: List[Tuple[str, str]],
    include_columns: List[Tuple[str, ...]] = None,
    input_property_filter: Properties = None,
    join_property_filter: Properties = None,
    input_aoi: dict = None,
    join_aoi: dict = None,
    client: Optional[VectorClient] = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Execute relational join between two Vector Tables.

    Parameters
    ----------
    input_product_id : str
        Product ID of the input Vector Table.
    join_product_id : str
        Product ID of the join Vector Table.
    join_type : str
        String indicating the type of join to perform.
        Must be one of INNER, LEFT, RIGHT, INTERSECTS,
            CONTAINS, OVERLAPS, WITHIN.
    join_columns : List[Tuple[str, str]]
        List of columns to join the input and join Vector Table.
        [(input_table.col1, join_table.col2), ...]
    include_columns : List[Tuple[str, ...]]
        List of columns to include from either side of
        the join formatted as [(input_table.col1, input_table.col2),
        (join_table.col3, join_table.col4)]. If None, all columns
        from both Vector Tables are returned.
    input_property_filter : Properties
        Property filters to filter the input Vector Table.
    join_property_filter : Properties
        Property filters to filter the join Vector Table.
    input_aoi : dict
        A GeoJSON Feature to filter the input Vector Table.
    join_aoi : dict
        A GeoJSON Feature to filter the join Vector Table.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """

    params = {
        "input_product_id": input_product_id,
        "join_type": join_type,
        "join_product_id": join_product_id,
        "join_columns": join_columns,
        "include_columns": include_columns,
        "input_property_filter": input_property_filter,
        "input_aoi": input_aoi,
        "join_property_filter": join_property_filter,
        "join_aoi": join_aoi,
        "keep_all_input_rows": False,  # not used with non-spatial join
    }

    if client is None:
        client = VectorClient.get_default_client()

    return _join(params, client)


def sjoin(
    input_product_id: str,
    join_product_id: str,
    join_type: str,
    include_columns: List[Tuple[str, ...]] = None,
    input_property_filter: Properties = None,
    join_property_filter: Properties = None,
    input_aoi: dict = None,
    join_aoi: dict = None,
    keep_all_input_rows: bool = False,
    client: Optional[VectorClient] = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Execute spatial join between two Vector Tables.

    Parameters
    ----------
    input_product_id : str
        Product ID of the input Vector Table.
    join_product_id : str
        Product ID of the join Vector Table.
    join_type : str
        String indicating the type of join to perform.
        Must be one of INNER, LEFT, RIGHT, INTERSECTS,
            CONTAINS, OVERLAPS, WITHIN.
    include_columns : List[Tuple[str, ...]]
        List of columns to include from either side of
        the join formatted as [(input_table.col1, input_table.col2),
        (join_table.col3, join_table.col4)]. If None, all columns
        from both Vector Tables are returned.
    input_property_filter : Properties
        Property filters to filter the input Vector Table.
    join_property_filter : Properties
        Property filters to filter the join Vector Table.
    input_aoi : dict
        A GeoJSON Feature to filter the input Vector Table.
    join_aoi : dict
        A GeoJSON Feature to filter the join Vector Table.
    keep_all_input_rows : bool
        Boolean indicating if the spatial join should keep all input rows
        whether they satisfy the spatial query or not.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """

    params = {
        "input_product_id": input_product_id,
        "join_type": join_type,
        "join_product_id": join_product_id,
        "join_columns": None,  # not used with spatial join
        "include_columns": include_columns,
        "input_property_filter": input_property_filter,
        "input_aoi": input_aoi,
        "join_property_filter": join_property_filter,
        "join_aoi": join_aoi,
        "keep_all_input_rows": keep_all_input_rows,
    }

    if client is None:
        client = VectorClient.get_default_client()

    return _join(params, client=client)


def get(
    product_id: str,
    feature_id: str,
    client: Optional[VectorClient] = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Get a feature from a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    feature_id : str
        ID of the feature.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
        A Pandas or GeoPandas dataframe.
    """
    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.get(
        f"/products/{product_id}/features/{feature_id}",
        params={"format": "Parquet"},
    )

    return response_to_dataframe(response=response)


@deprecate(removed=["is_spatial"])
def update(
    product_id: str,
    feature_id: str,
    dataframe: Union[gpd.GeoDataFrame, pd.DataFrame],
    is_spatial: Optional[bool] = None,
    client: Optional[VectorClient] = None,
) -> None:
    """
    Save/update a feature in a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    feature_id : str
        ID of the feature.
    dataframe : Union[gpd.GeoDataFrame, pd.DataFrame]
        A GeoPandas GeoDataFrame or a Pandas DataFrame to replace
        the feature with.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    None
    """
    if not isinstance(dataframe, TYPES):
        raise TypeError(f"Unsupported data type {type(dataframe)}")

    if dataframe.shape[0] != 1:
        raise ValueError("Only 1 row can be updated")

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("vector.parquet", buffer, "application/octet-stream")}

    if client is None:
        client = VectorClient.get_default_client()

    # The client session normally hardwires the content type.
    # We need to remove it for this request so that the multipart
    # support in urllib3 can set it appropriately.
    session = client.session
    content_type = session.headers.get("content-type")
    if content_type is not None:
        del session.headers["content-type"]
    try:
        session.put(
            f"/products/{product_id}/featuresv2/{feature_id}",
            files=files,
        )
    finally:
        if content_type is not None:
            session.headers.update({"content-type": content_type})


def aggregate(
    product_id: str,
    statistic: Statistic,
    property_filter: Properties = None,
    aoi: dict = None,
    columns: list = None,
    client: Optional[VectorClient] = None,
) -> Union[int, dict]:
    """
    Calculate aggregate statistics for features in a Vector Table.
    The statistic COUNT will always return an integer. All other
    statistics will return a dictionary of results. Keys of the
    dictionary will be the column names requested appended with
    the statistic ('column_1.STATISTIC') and values are the result
    of the aggregate statistic.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table
    statistic : Statistic
        Statistic to calculate.
    property_filter : Properties, optional
        Property filters to filter the product with.
    aoi : dict, optional
        A GeoJSON Feature to filter the vector product with.
    columns : list, optional
        Optional list of column names.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    Union[int, dict]
    """
    if not isinstance(statistic, Statistic):
        raise TypeError("'statistic' must be of type <Statistic>.")

    if property_filter is not None:
        property_filter = property_filter.serialize()

    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.post(
        f"/products/{product_id}/features/aggregate",
        json={
            "statistic": statistic.value,
            "filter": property_filter,
            "aoi": aoi,
            "columns": columns,
        },
        timeout=(client.CONNECT_TIMEOUT, client.READ_TIMEOUT),
    )

    return response.json()


def delete(
    product_id: str,
    feature_id: str,
    client: Optional[VectorClient] = None,
):
    """
    Delete a feature in a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    feature_id : str
        ID of the feature.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.
    """

    if client is None:
        client = VectorClient.get_default_client()

    client.session.delete(
        f"/products/{product_id}/features/{feature_id}",
    )
