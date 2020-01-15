# jsonapi_document


import json
import os

from descarteslabs.client.auth import Auth
from descarteslabs.client.exceptions import ClientError
from descarteslabs.client.services.service.service import (
    JsonApiService,
    JsonApiSession,
    HttpRequestMethod,
)


HttpRequestMethod = HttpRequestMethod


class _RewriteErrorSession(JsonApiSession):
    """Rewrite JSON ClientErrors that are returned to make them easier to read"""

    def request(self, *args, **kwargs):
        try:
            return super(_RewriteErrorSession, self).request(*args, **kwargs)
        except ClientError as client_error:
            self._rewrite_error(client_error)
            raise

    def _rewrite_error(self, client_error):
        KEY_ERRORS = "errors"
        KEY_TITLE = "title"
        KEY_STATUS = "status"
        KEY_DETAIL = "detail"
        KEY_SOURCE = "source"
        KEY_POINTER = "pointer"
        message = ""

        for arg in client_error.args:
            try:
                errors = json.loads(arg)[KEY_ERRORS]

                for error in errors:
                    line = ""
                    seperator = ""

                    if KEY_TITLE in error:
                        line += error[KEY_TITLE]
                        seperator = ": "
                    elif KEY_STATUS in error:
                        line += error[KEY_STATUS]
                        seperator = ": "

                    if KEY_DETAIL in error:
                        line += seperator + error[KEY_DETAIL].strip(".")
                        seperator = ": "

                    if KEY_SOURCE in error:
                        source = error[KEY_SOURCE]
                        if KEY_POINTER in source:
                            source = source[KEY_POINTER].split("/")[-1]
                        line += seperator + source

                    if line:
                        message += "\n    " + line
            except Exception:
                return

        if message:
            client_error.args = (message,)


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
        not set, the logic will first look for the environment variable
        ``DESCARTESLABS_CATALOG_V2_URL`` and then use the default Descartes Labs
        catalog.
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
            auth = Auth()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_CATALOG_V2_URL",
                "https://platform.descarteslabs.com/metadata/v1/catalog/v2",
            )

        super(CatalogClient, self).__init__(
            url, auth=auth, retries=retries, session_class=_RewriteErrorSession
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
