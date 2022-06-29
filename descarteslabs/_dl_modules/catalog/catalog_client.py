# jsonapi_document


from descarteslabs.auth import Auth
from descarteslabs.config import get_settings
from ..client.services.service.service import (
    JsonApiService,
    HttpRequestMethod,
)


HttpRequestMethod = HttpRequestMethod


class CatalogClient(JsonApiService):
    """
    The CatalogClient handles the HTTP communication with the Descartes Labs catalog.
    It is almost sufficient to use the default client that is automatically retrieved
    using `get_default_client`.  However, if you want to adjust e.g.  the retries, you
    can create your own.

    Parameters
    ----------
    url : str, optional
        The URL to use when connecting to the Descartes Labs catalog.  Only change
        this if you are being asked to use a non-default Descartes Labs catalog.  If
        not set, then ``descarteslabs.config.get_settings().CATALOG_V2_URL`` will be used.
    auth : Auth, optional
        The authentication object used when connecting to the Descartes Labs catalog.
        This is typically the default `Auth` object that uses the cached authentication
        token retrieved with the shell command "$ descarteslabs auth login".
    retries : int, optional
        The number of retries when there is a problem with the connection.  Set this to
        zero to disable retries.  The default is 3 retries.
    """

    _instance = None

    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().catalog_v2_url

        super(CatalogClient, self).__init__(
            url, auth=auth, retries=retries, rewrite_errors=True
        )

    @staticmethod
    def get_default_client():
        """Retrieve the default client.

        This client is used whenever you don't explicitly set the client.
        """
        if CatalogClient._instance is None:
            CatalogClient._instance = CatalogClient()

        return CatalogClient._instance

    @staticmethod
    def set_default_client(client):
        """Change the default client to the given client.

        This is the client that will be used whenever you don't explicitly set the
        client
        """
        CatalogClient._instance = client
