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

import io
from typing import Union

import geopandas as gpd
import pandas as pd
import requests

from descarteslabs.exceptions import ServerError


def response_to_dataframe(
    response: requests.Response,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Function to convert the content of a response to
    Pandas DataFrame or GeoPandas GeoDataFrame.

    Parameters
    ----------
    response: requests.Response
        Response object from requests call.

    Returns
    -------
    Union[pd.DataFrame, gpd.GeoDataFrame]
    """
    buffer = io.BytesIO(response.content)

    is_spatial = response.headers.get("is_spatial", "false").lower() == "true"

    try:
        if is_spatial:
            return gpd.read_parquet(buffer)
        else:
            return pd.read_parquet(buffer)
    except Exception as e:
        raise ServerError(f"Unable to convert response to DataFrame: {e}")
