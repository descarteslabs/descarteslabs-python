import json
from typing import Optional

from descarteslabs.exceptions import ClientError, ServerError

from .service import HttpHeaderKeys, HttpHeaderValues, Service, Session


class ApiSession(Session):
    def __init__(self, *args, **kwargs):
        self.rewrite_errors = True
        super().__init__(*args, **kwargs)

    def request(self, *args, **kwargs):
        """Sends an HTTP request and emits Descartes Labs specific errors.

        Parameters
        ----------
        method: str
            The HTTP method to use.
        url: str
            The URL to send the request to.
        kwargs: dict
            Additional arguments.  See `requests.request
            <https://requests.readthedocs.io/en/master/api/#requests.request>`_.

        Returns
        -------
        Response
            A :py:class:`request.Response` object.

        Raises
        ------
        BadRequestError
            Either a 400 or 422 HTTP response status code was encountered.
        ~descarteslabs.exceptions.NotFoundError
            A 404 HTTP response status code was encountered.
        ProxyAuthenticationRequiredError
            A 407 HTTP response status code was encountered indicating proxy
            authentication was not handled or was invalid.
        ConflictError
            A 409 HTTP response status code was encountered.
        ValidationError
            A 422 HTTP response status code was encountered.
            ValidationError extends BadRequestError for backward compatibility.
        RateLimitError
            A 429 HTTP response status code was encountered.
        GatewayTimeoutError
            A 504 HTTP response status code was encountered.
        ~descarteslabs.exceptions.ServerError
            Any HTTP response status code larger than 400 that was not covered above
            is returned as a ServerError.  The original HTTP response status code
            can be found in the attribute :py:attr:`original_status`.

        Note
        ----
        If :py:attr:`rewrite_errors` was set to ``True`` in the corresponding
        :py:class:`ApiService`, the API errors will be rewritten in a more
        human readable format.
        """

        try:
            resp = super(ApiSession, self).request(*args, **kwargs)
        except (ClientError, ServerError) as error:
            if self.rewrite_errors:
                self._rewrite_error(error)
            raise

        return resp

    def _rewrite_error(self, exception: Exception):
        """All errors contain just a `detail` key at the moment.

        Validation errors are a special case with the format:

        .. code::

            {
                "detail": "Invalid request",
                "errors": {
                    "field1": ["error 1", "error 2"],
                    "field2": ["some error"]
                }
            }
        """
        indent = "    "
        message = ""

        for error in exception.args:
            try:
                json_error = json.loads(error)

                if "detail" in json_error:
                    message += "\n" + json_error["detail"]

                if "errors" in json_error:
                    message += ":\n"

                    for field, errors in json_error["errors"].items():
                        message += f"{indent}{field}\n"

                        for field_error in errors:
                            message += f"{indent * 2}{field_error}\n"
                else:
                    message += "\n"
            except Exception:
                return

        message = message.rstrip()

        if message:
            exception.args = (message,)


class ApiService(Service):
    """A FastAPI oriented default Descartes Labs HTTP Service.

    For details see the :py:class:`Service`.

    This service uses the :py:class:`ApiSession` which provides some optional
    functionality.

    This functionality currently rewrites JSON errors to a human readable format.

    Parameters
    ----------
    url: str
        The URL prefix to use for communication with the Descartes Labs servers.
    session_class: class
        The session class to use when instantiating the session.  This must be a derived
        class from :py:class:`ApiSession`.  If not provided, the default session
        class is used.  You can register a default session class with
        :py:meth:`ApiService.set_default_session_class`.
    rewrite_errors: bool
        When set to ``True``, errors are rewritten to be more readable. Each API
        error becomes it's own line.
    auth: Auth, optional
        A Descartes Labs :py:class:`~descarteslabs.auth.Auth` instance.  If not
        provided, a default one will be instantiated.
    retries: int or urllib3.util.retry.Retry If a number, it's the number of retries
        that will be attempted.  If a :py:class:`urllib3.util.retry.Retry` instance,
        it will determine the retry behavior.  If not provided, the default retry
        policy as described above will be used.
    """

    _session_class = ApiSession

    def __init__(self, url, session_class=None, rewrite_errors=True, **kwargs):
        if not (session_class is None or issubclass(session_class, ApiSession)):
            raise TypeError(
                "The session class must be a subclass of {}.".format(ApiSession)
            )

        self.rewrite_errors = rewrite_errors
        super(ApiService, self).__init__(url, session_class=session_class, **kwargs)

    @classmethod
    def set_default_session_class(cls, session_class):
        """Set the default session class for :py:class:`ApiService`.

        The default session is used for any :py:class:`ApiService` that is
        instantiated without specifying the session class.

        Parameters
        ----------
        session_class: class
            The session class to use when instantiating the session.  This must be the
            class :py:class:`ApiSession` itself or a derived class from
            :py:class:`ApiSession`.
        """

        if not issubclass(session_class, ApiSession):
            raise TypeError(
                "The session class must be a subclass of {}.".format(ApiSession)
            )

        cls._session_class = session_class

    @classmethod
    def get_default_session_class(cls):
        """Get the default session class for :py:class:`ApiService`.

        Returns
        -------
        ApiService
            The default session class, which is :py:class:`ApiService` itself or
            a derived class from :py:class:`ApiService`.
        """

        return cls._session_class

    def _build_session(self):
        session: ApiSession = super(ApiService, self)._build_session()
        session.rewrite_errors = self.rewrite_errors
        session.headers.update(
            {
                HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationJson,
                HttpHeaderKeys.Accept: HttpHeaderValues.ApplicationJson,
            }
        )
        return session

    def iter_pages(self, url: str, params: Optional[dict] = None):
        if params is None:
            params = {}

        while True:
            response = self.session.get(url, params=params)
            response_json = response.json()
            data = response_json["data"]
            next_page = response_json["meta"]["page_cursor"]

            for item in data:
                yield item

            if not next_page:
                break

            params = {"page_cursor": next_page}
