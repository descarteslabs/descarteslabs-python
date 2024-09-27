# Copyright 2018-2024 Descartes Labs.

import json
import urllib.parse
from typing import List, Optional

from ..common.property_filtering import Properties

from .layers import DLVectorTileLayer
from .vector import Table
from .vector_client import VectorClient


def create_layer(
    product_id: str,
    name: str,
    property_filter: Optional[Properties] = None,
    columns: Optional[List[str]] = None,
    vector_tile_layer_styles: Optional[dict] = None,
    client: Optional[VectorClient] = None,
) -> DLVectorTileLayer:
    """
    Create vector tile layer from a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    name : str
        Name to give to the ipyleaflet vector tile layer.
    property_filter : Properties, optional
        Property filter to apply to the vector tiles.
    columns : list of str, optional
       Optional list of column names to include. These can be used for styling.
    vector_tile_layer_styles : dict, optional
        Vector tile styles to apply. See https://ipyleaflet.readthedocs.io/en/latest/layers/vector_tile.html for more
        details.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    DLVectorTileLayer
    """
    if client is None:
        client = VectorClient.get_default_client()

    # Error if the table is not found or spatial
    table = Table.get(product_id, client=client)

    if not table.is_spatial:
        raise ValueError(f"'{product_id}' is not a spatially enabled Vector Table")

    # Initialize vector tile layer styles if no styles are provided
    if vector_tile_layer_styles is None:
        vector_tile_layer_styles = {}

    # Initialize the property filter if none is provided
    if property_filter is not None:
        property_filter = property_filter.serialize()

    # Construct the query parameters
    property_filter = json.dumps(property_filter)
    columns = json.dumps(columns)
    query_params = urllib.parse.urlencode(
        {
            "property_filter": property_filter,
            "columns": columns,
        },
        doseq=True,
    )

    # Create an ipyleaflet vector tile layer and return it
    lyr = DLVectorTileLayer(
        url=f"{client.base_url}/products/{product_id}/tiles/{{z}}/{{x}}/{{y}}?{query_params}",
        name=name,
        vector_tile_layer_styles=vector_tile_layer_styles,
    )
    return lyr
