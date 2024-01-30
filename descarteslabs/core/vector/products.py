from typing import List, Optional, Union

from ..common.vector.models import GenericFeatureBaseModel, VectorBaseModel

from .vector_client import VectorClient


def _check_tags(tags: Union[List[str], None] = None):
    if tags:
        for tag in tags:
            if tag.find(",") >= 0:
                raise ValueError('tags cannot contain ","')


def _strip_null_values(d: dict) -> dict:
    """Strip null (ie. None) values from a dictionary.

    This is used to strip null values from request query strings.

    Parameters
    ----------
    d : dict
        The input dictionary.

    Returns
    -------
    dict
    """
    return {k: v for k, v in d.items() if v is not None}


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
) -> dict:
    """
    Create a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.
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
        A json schema describing the table
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    dict
    """
    _check_tags(tags)

    if "geometry" in model.model_json_schema()["properties"].keys():
        is_spatial = True
    else:
        is_spatial = False

    request_json = _strip_null_values(
        {
            "id": product_id,
            "name": name,
            "is_spatial": is_spatial,
            "description": description,
            "tags": tags,
            "readers": readers,
            "writers": writers,
            "owners": owners,
            "model": model.model_json_schema(),
        }
    )

    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.post("/products/", json=request_json)

    return response.json()


def list(
    tags: Union[List[str], None] = None,
    client: Optional[VectorClient] = None,
) -> List[dict]:
    """
    List Vector Tables.

    Parameters
    ----------
    tags: List[str]
        Optional list of tags a Vector Table must have to be included in the returned list.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    List[dict]
    """
    _check_tags(tags)

    params = None
    if tags:
        params = {"tags": ",".join(tags)}

    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.get("/products/", params=params)

    return response.json()


def get(
    product_id: str,
    client: Optional[VectorClient] = None,
) -> dict:
    """
    Get a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    dict
    """

    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.get(f"/products/{product_id}")

    return response.json()


def update(
    product_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    readers: Optional[List[str]] = None,
    writers: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    client: Optional[VectorClient] = None,
) -> dict:
    """
    Save/update a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.
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
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    dict
    """
    _check_tags(tags)

    if client is None:
        client = VectorClient.get_default_client()

    response = client.session.patch(
        f"/products/{product_id}",
        json=_strip_null_values(
            {
                "name": name,
                "description": description,
                "tags": tags,
                "readers": readers,
                "writers": writers,
                "owners": owners,
            },
        ),
    )

    return response.json()


def delete(
    product_id: str,
    client: Optional[VectorClient] = None,
) -> None:
    """
    Delete a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.
    client : VectorClient, optional
        Client to use for requests. If not provided, the default client will be used.

    Returns
    -------
    None
    """
    if client is None:
        client = VectorClient.get_default_client()

    client.session.delete(f"/products/{product_id}")
