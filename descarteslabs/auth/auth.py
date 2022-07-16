# Copyright 2018-2020 Descartes Labs.
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

import base64
import datetime
import errno
import json
import os
import random
import stat
import tempfile
import threading
import warnings
from hashlib import sha1

import requests
from descarteslabs.exceptions import AuthError, OauthError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# copied from descarteslabs/common/threading/local.py, but we need
# it standalone here to avoid any dependencies on our own packages
# for client configuration purposes
class ThreadLocalWrapper(object):
    """
    A wrapper around a thread-local object that gets created lazily in every
    thread of every process via the given factory callable when it is
    accessed. I.e., at most one instance per thread exists.

    In contrast to standard thread-locals this is compatible with multiple
    processes.
    """

    def __init__(self, factory):
        self._factory = factory
        self._create_local(os.getpid())

    def get(self):
        self._init_local()
        if not hasattr(self._local, "wrapped"):
            self._local.wrapped = self._factory()
        return self._local.wrapped

    def _init_local(self):
        local_pid = os.getpid()
        previous_pid = getattr(self._local, "_pid", None)
        if previous_pid is None:
            self._local._pid = local_pid
        elif local_pid != previous_pid:
            self._create_local(local_pid)

    def _create_local(self, pid):
        self._local = threading.local()
        self._local._pid = pid


DEFAULT_TOKEN_INFO_DIR = os.path.join(os.path.expanduser("~"), ".descarteslabs")
DEFAULT_TOKEN_INFO_PATH = os.path.join(DEFAULT_TOKEN_INFO_DIR, "token_info.json")
JWT_TOKEN_PREFIX = "jwt_token_"
DESCARTESLABS_CLIENT_ID = "DESCARTESLABS_CLIENT_ID"
DESCARTESLABS_CLIENT_SECRET = "DESCARTESLABS_CLIENT_SECRET"
DESCARTESLABS_REFRESH_TOKEN = "DESCARTESLABS_REFRESH_TOKEN"
DESCARTESLABS_TOKEN = "DESCARTESLABS_TOKEN"

DESCARTESLABS_TOKEN_INFO_PATH = "DESCARTESLABS_TOKEN_INFO_PATH"


def base64url_decode(input):
    """Helper method to base64url_decode a string.

    Parameter
    ---------
    input : str
        A base64url_encoded string to decode.
    """
    rem = len(input) % 4
    if rem > 0:
        input += b"=" * (4 - rem)

    return base64.urlsafe_b64decode(input)


def makedirs_if_not_exists(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except OSError as ex:
            if ex.errno == errno.EEXIST:
                pass
            else:
                raise


class Auth:
    """Client used to authenticate with all Descartes Labs service APIs."""

    RETRY_CONFIG = Retry(
        total=5,
        backoff_factor=random.uniform(1, 10),
        allowed_methods=frozenset(["GET", "POST"]),
        status_forcelist=[429, 500, 502, 503, 504],
    )

    ADAPTER = ThreadLocalWrapper(lambda: HTTPAdapter(max_retries=Auth.RETRY_CONFIG))

    AUTHORIZATION_ERROR = (
        "No valid authentication info found{}. "
        "See https://docs.descarteslabs.com/authentication.html."
    )

    KEY_CLIENT_ID = "client_id"
    KEY_CLIENT_SECRET = "client_secret"
    KEY_REFRESH_TOKEN = "refresh_token"
    KEY_SCOPE = "scope"
    KEY_GRANT_TYPE = "grant_type"
    KEY_TARGET = "target"
    KEY_API_TYPE = "api_type"
    KEY_JWT_TOKEN = "jwt_token"
    KEY_ALT_JWT_TOKEN = "JWT_TOKEN"

    __attrs__ = [
        "domain",
        "scope",
        "leeway",
        "token_info_path",
        "client_id",
        "client_secret",
        "refresh_token",
        "_token",
        "_namespace",
        "RETRY_CONFIG",
    ]

    _default_token_info_path = object()  # Just any unique object

    _instance = None  # the default Auth instance

    def __init__(
        self,
        domain="https://accounts.descarteslabs.com",
        scope=None,
        leeway=500,
        token_info_path=_default_token_info_path,
        client_id=None,
        client_secret=None,
        jwt_token=None,
        refresh_token=None,
        retries=None,
        _suppress_warning=False,
    ):
        """Retrieves a JWT access token from a client id and refresh token for cli usage.

        By default and without arguments the credentials are retrieved from a
        config file named ``token_info.json``. This file can be created by running
        ``descarteslabs auth login`` from the command line.

        You can change the default location by setting the environment variable
        ``DESCARTESLABS_TOKEN_INFO_PATH``. Make sure you do this **before** running
        ``descarteslabs auth login`` so the credentials will be saved to the file
        specified in the environment variable, and when still set when instantiating
        this class, the credentials will be read from that file.

        To use a short-lived access token that will not be refreshed, either set the
        environment variable ``DESCARTESLABS_TOKEN`` or use the ``jwt_token`` parameter.

        To use a long-lived refresh token that will be refreshed, either set the
        environment variables ``DESCARTESLABS_CLIENT_ID`` and
        ``DESCARTESLABS_CLIENT_SECRET`` or use the parameters ``client_id`` and
        ``client_secret``. This will retrieve an access token which will be cached
        between instances for the same combination of client id and client secret.

        If in addition to the client id and client secret you also specify a valid
        short-lived access token, it will be used until it expires.

        Note that the environment variable ``DESCARTESLABS_REFRESH_TOKEN`` is identical
        to ``DESCARTESLABS_CLIENT_SECRET`` and the parameter ``refresh_token`` is
        identical to ``client_secret``. Use one or the other but not both.

        Although discouraged, it is possible to set one value as environment variable,
        and pass the other value in as parameter. For example, one could set the
        environment variable ``DESCARTESLABS_CLIENT_ID`` and only pass in the parameter
        ``client_secret``.

        If you also specify a ``token_info_path`` that indicates which file to
        read the credentials from. If used by itself, it works the same as
        ``DESCARTESLABS_TOKEN_INFO_PATH`` and assuming the file exists and contains
        valid credentials, you could switch between accounts this way.

        If you specify the ``token_info_path`` together with an additional
        client id and client secret (whether retrieved through environment
        variables or given using parameters), the given credentials will be
        written to the given file. If this file already exists and contains
        matching credentials, it will be used to retrieve the short-lived
        access token and refreshes it when it expires. If the file already
        exists and contains conflicting credentials, it will be overwritten
        with the new credentials.

        Parameters
        ----------

        domain : str, default ``https://accounts.descarteslabs.com``
            The domain used for the credentials. You should normally never
            change this.
        scope : list(str), optional
            The JWT access token fields to be included. You should normally
            never have to use this.
        leeway : int, default 500
            The leeway is given in seconds and is used as a safety cushion
            for the expiration. If the expiration falls within the leeway,
            the JWT access token will be renewed.
        token_info_path : str, default ``~/.descarteslabs/token_info.json``
            Path to a JSON file holding the credentials. If not set and
            credentials are provided through environment variables or through
            parameters, this parameter will **not** be used. However, if no
            credentials are provided through environment variables or through
            parameters, it will default to ``~/.descarteslabs/token_info.json``
            and credentials will be retrieved from that file if present. If
            explicitly set to ``None``, credentials will never be retrieved
            from file and **must** be provided through environment variables
            or parameters.
        client_id : str, optional
            The JWT client id. If provided it will take precedence over the
            corresponding environment variable, or the credentials retrieved through
            the file specified in ``token_info_path``. If this parameter is provided,
            you **must** either provide a ``client_id`` or ``refresh_token`` (but not
            both). Access tokens retrieved this way will be cached without revealing
            the client secret.
        client_secret : str, optional
            The refresh token used to retrieve short-lived access tokens. If provided
            it will take precedence over the corresponding environment variable, or the
            credentials retrieved through the file specified in ``token_info_path``. If
            this parameter is provided, you **must** also provide a client id either as
            a parameter or through an environment variable. Access tokens retrieved this
            way will be cached without revealing the client secret.
        jwt_token : str, optional
            A short-lived JWT access token. If valid and used without other parameters,
            it will be used for access. If used with a client id, the access token must
            match or it will be discarded. If the access token is discarded either
            because it expired or didn't match the given client id, and no client secret
            has been given, no new access token can be retrieved and access will be
            denied. If used with both client id and client secret, the token will be
            cached and updated as needed without revealing the client secret.
        refresh_token : str, optional
            Identical to the ``client_secret``. You can only specify one or the other,
            or if specified both, they must match. The refresh token takes precedence
            over the client secret.
        retries : Retry or int, optional
            The number of retries and backoff policy;
            by default 5 retries with a random backoff policy between 1 and 10 seconds.

        Raises
        ------
        UserWarning
            In case the refresh token and client secret differ.
            In case the defailt or given ``token_info_path`` cannot be found.
            In case no credentials can be found.

        Examples
        --------
        >>> import descarteslabs
        >>> # Use default credentials obtained through 'descarteslabs auth login'
        >>> auth = descarteslabs.auth.Auth()
        >>> # Your Descartes Labs user id
        >>> auth.namespace # doctest: +SKIP
        'a54d88e06612d820bc3be72877c74f257b561b19'
        >>> auth = descarteslabs.auth.Auth(
        ...     client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        ...     client_secret="b70B_ozH6CaV23WQ-toFQ8CaujGHDs-eC39QEJTRnZa9Z",
        ... )
        >>> auth.namespace # doctest: +SKIP
        '67f21eb1040f978fe1da32e5e33501d0f4a604ac'
        >>>
        """

        # The logic here is murky and changed over time. Initially, the logic would
        # retrieve *any* of the information from *any* of the sources. This resulted in
        # the `token_info.json` being overwritten when you would use a different refresh
        # token set in the environment or passed in. This was changed to make a
        # distinction between data that is provided through the environment or as
        # arguments, versus the data that is retrieved from `token_info.json`. This still
        # allows arbitrary combinations of data provided through the environment and
        # passed in as arguments.

        # In addition there are duplicate keys and arguments, which makes things even
        # more unnecessarily complicated. For backward compatibility reasons we keep it
        # as-is. Overall the core information consists of:
        #     client_id:     The oauth application id.
        #     client_secret: Same as refresh_token.
        #     refresh_token: The oauth application refresh token. Refresh token has
        #                    precedence over client_secret.
        #     _token:        The short-lived jwt id token that can be generated from the
        #                    refresh token if present.

        self.token_info_path = token_info_path

        if token_info_path is Auth._default_token_info_path:
            token_info_path = None
            self.token_info_path = os.environ.get(
                DESCARTESLABS_TOKEN_INFO_PATH, DEFAULT_TOKEN_INFO_PATH
            )

        token_info = {}

        # First determine if we are getting our info from the args or environment
        self.client_id = next(
            (
                x
                for x in (
                    client_id,
                    os.environ.get(DESCARTESLABS_CLIENT_ID),
                    os.environ.get("CLIENT_ID"),
                )
                if x is not None
            ),
            None,
        )

        self.client_secret = next(
            (
                x
                for x in (
                    client_secret,
                    os.environ.get(DESCARTESLABS_CLIENT_SECRET),
                    os.environ.get("CLIENT_SECRET"),
                )
                if x is not None
            ),
            None,
        )

        self.refresh_token = next(
            (
                x
                for x in (
                    refresh_token,
                    os.environ.get(DESCARTESLABS_REFRESH_TOKEN),
                )
                if x is not None
            ),
            None,
        )

        self._token = next(
            (
                x
                for x in (
                    jwt_token,
                    os.environ.get(DESCARTESLABS_TOKEN),
                )
                if x is not None
            ),
            None,
        )

        # Make sure self.refresh_token is set
        if not self.refresh_token:
            self.refresh_token = self.client_secret

        if self.client_id or self.refresh_token or self._token:
            # Information is provided through the environment or as argument
            if token_info_path:
                # Explicit token_info.json file; see if we can use it...
                if os.path.exists(self.token_info_path):
                    token_info = self._read_token_info(self.token_info_path)

                    if (
                        not self._token
                        and self.client_id == token_info.get(self.KEY_CLIENT_ID)
                        and self.refresh_token == token_info.get(self.KEY_REFRESH_TOKEN)
                    ):
                        self._token = token_info.get(self.KEY_JWT_TOKEN)
            elif self.refresh_token:
                # Make the saved JWT token file unique to the refresh token
                token = self.refresh_token
                token_sha1 = sha1(token.encode("utf-8")).hexdigest()
                self.token_info_path = os.path.join(
                    DEFAULT_TOKEN_INFO_DIR, f"{JWT_TOKEN_PREFIX}{token_sha1}.json"
                )

                if self._token:
                    self._write_token_info(
                        self.token_info_path, {self.KEY_JWT_TOKEN: self._token}
                    )
                else:
                    self._token = self._read_token_info(
                        self.token_info_path, suppress_warning=True
                    ).get(self.KEY_JWT_TOKEN)
        elif self.token_info_path:
            # All information comes from the cached token_info.json file
            token_info = self._read_token_info(self.token_info_path, _suppress_warning)

            self.client_id = token_info.get(self.KEY_CLIENT_ID)
            self.client_secret = token_info.get(self.KEY_CLIENT_SECRET)
            self.refresh_token = token_info.get(self.KEY_REFRESH_TOKEN)
            self._token = next(
                (
                    x
                    for x in (
                        token_info.get(self.KEY_ALT_JWT_TOKEN),
                        token_info.get(self.KEY_JWT_TOKEN),
                    )
                    if x is not None
                ),
                None,
            )

        # The refresh token and client secret should be identical if both set
        if (
            self.client_secret
            and self.refresh_token
            and self.client_secret != self.refresh_token
        ):
            warnings.warn(
                "Authentication token mismatch: both the client secret and the "
                "refresh token are provided but differ in value; "
                "the refresh token will be used for authentication.",
                stacklevel=2,
            )

        # Make sure they're identical. Refresh token has precedence.
        if self.refresh_token:
            self.client_secret = self.refresh_token
        elif self.client_secret:
            self.refresh_token = self.client_secret

        self.scope = next(
            (x for x in (scope, token_info.get(self.KEY_SCOPE)) if x is not None), None
        )

        # Verify that the token is valid; otherwise clear it
        if self._token:
            try:
                payload = self._get_payload(self._token)
            except AuthError:
                self._token = None
            else:
                if self._token_expired(payload) or (
                    self.client_id and payload.get("aud") != self.client_id
                ):
                    self._token = None

        if not _suppress_warning and not (
            self._token or (self.client_id and self.refresh_token)
        ):
            # Won't authn if we don't have a token or a client_id/refresh_token pair
            warnings.warn(self.AUTHORIZATION_ERROR.format(""), stacklevel=2)

        self._namespace = None

        if retries is None:
            self._adapter = self.ADAPTER
        else:
            self.RETRY_CONFIG = retries
            self._init_adapter()

        self._init_session()
        self.domain = domain
        self.leeway = leeway

    @classmethod
    def from_environment_or_token_json(cls, **kwargs):
        """Creates an Auth object from the given arguments.

        Creates an Auth object from the given arguments,
        environment variables, or stored credentials.

        See :py:class:`Auth` for details.
        """
        return Auth(**kwargs)

    def _init_adapter(self):
        self._adapter = ThreadLocalWrapper(
            lambda: HTTPAdapter(max_retries=self.RETRY_CONFIG)
        )

    def _init_session(self):
        # Sessions can't be shared across threads or processes because the underlying
        # SSL connection pool can't be shared. We create them thread-local to avoid
        # intractable exceptions when users naively share clients e.g. when using
        # multiprocessing.
        self._session = ThreadLocalWrapper(self.build_session)

    def _token_expired(self, payload, leeway=0):
        exp = payload.get("exp")

        if exp is not None:
            now = (
                datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)
            ).total_seconds()

            return now + leeway > exp

        return True  # Must have exp

    @property
    def token(self):
        """Gets the short-lived JWT access token.

        Returns
        -------
        str
            The JWT token string.

        Raises
        ------
        AuthError
            Raised when incomplete credentials were provided.
        OauthError
            Raised when a token cannot be obtained or refreshed.
        """
        if self._token is None:
            self._get_token()
        else:  # might have token but could be close to expiration
            payload = self._get_payload(self._token)

            if self._token_expired(payload, self.leeway):
                try:
                    self._get_token()
                except AuthError as e:
                    # Unable to refresh, raise if truly expired
                    if self._token_expired(payload):
                        raise e

        return self._token

    @property
    def payload(self):
        """Gets the token payload.

        Returns
        -------
        dict
            Dictionary containing the fields specified by scope, which may include:

            .. highlight:: none

            ::

                name:           The name of the user.
                groups:         Groups to which the user belongs.
                org:            The organization to which the user belongs.
                email:          The email address of the user.
                email_verified: True if the user's email has been verified.
                sub:            The user identifier.
                exp:            The expiration time of the token, in seconds since
                                the start of the unix epoch.

        Raises
        ------
        AuthError
            Raised when incomplete credentials were provided.
        OauthError
            Raised when a token cannot be obtained or refreshed.
        """
        return self._get_payload(self.token)

    @staticmethod
    def _get_payload(token):
        if isinstance(token, str):
            token = token.encode("utf-8")

        if isinstance(token, str):
            token = token.encode("utf-8")

        try:
            # Anything that goes wrong here means it's a bad token
            claims = token.split(b".")[1]
            return json.loads(base64url_decode(claims).decode("utf-8"))
        except Exception as e:
            raise AuthError("Unable to read token {}: {}".format(token, e))

    @property
    def session(self):
        return self._session.get()

    def build_session(self):
        session = requests.Session()
        session.mount("https://", self.ADAPTER.get())
        return session

    @staticmethod
    def get_default_auth():
        """Retrieve the default Auth.

        This Auth is used whenever you don't explicitly set the Auth
        when creating clients, etc.
        """
        if Auth._instance is None:
            Auth._instance = Auth()

        return Auth._instance

    @staticmethod
    def set_default_auth(auth):
        """Change the default Auth to the given Auth.

        This is the Auth that will be used whenever you don't explicitly set the
        Auth when creating clients, etc.
        """
        Auth._instance = auth

    @staticmethod
    def _read_token_info(path, suppress_warning=False):
        try:
            with open(path) as fp:
                return json.load(fp)
        except Exception as e:
            if not suppress_warning:
                warnings.warn(
                    "Unable to read token_info from {} with error {}.".format(
                        path, str(e)
                    ),
                    stacklevel=3,
                )

        return {}

    @staticmethod
    def _write_token_info(path, token_info):
        token_info_directory = os.path.dirname(path)
        temp_prefix = ".{}.".format(os.path.basename(path))

        fd = None
        temp_path = None
        suppress_warning = False

        try:
            if Auth.KEY_JWT_TOKEN in token_info:
                token = token_info[Auth.KEY_JWT_TOKEN]

                if isinstance(token, bytes):
                    token_info[Auth.KEY_JWT_TOKEN] = token.decode("utf-8")

            makedirs_if_not_exists(token_info_directory)
            fd, temp_path = tempfile.mkstemp(
                prefix=temp_prefix, dir=token_info_directory
            )

            if JWT_TOKEN_PREFIX in path:
                token_info = {Auth.KEY_JWT_TOKEN: token_info[Auth.KEY_JWT_TOKEN]}
                suppress_warning = True

            try:
                with os.fdopen(fd, "w+") as fp:
                    json.dump(token_info, fp)
            finally:
                fd = None  # Closed now

            os.chmod(temp_path, stat.S_IRUSR | stat.S_IWUSR)

            try:
                os.rename(temp_path, path)
            except FileExistsError:
                # On windows remove the file first
                os.remove(path)
                os.rename(temp_path, path)
        except Exception as e:
            if not suppress_warning:
                warnings.warn(
                    "Failed to save token: {}".format(e),
                    stacklevel=3,
                )
        finally:
            if fd is not None:
                os.close(fd)

            if temp_path is not None and os.path.exists(temp_path):
                os.remove(temp_path)

    def _get_token(self, timeout=100):
        if self.client_id is None:
            raise AuthError(self.AUTHORIZATION_ERROR.format(" (no client_id)"))

        if self.client_secret is None and self.refresh_token is None:
            raise AuthError(
                self.AUTHORIZATION_ERROR.format(" (no client_secret or refresh_token)")
            )

        if self.client_id in [
            "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        ]:  # TODO(justin) remove legacy handling
            # TODO (justin) insert deprecation warning
            if self.scope is None:
                scope = ["openid", "name", "groups", "org", "email"]
            else:
                scope = self.scope
            params = {
                self.KEY_SCOPE: " ".join(scope),
                self.KEY_CLIENT_ID: self.client_id,
                self.KEY_GRANT_TYPE: "urn:ietf:params:oauth:grant-type:jwt-bearer",
                self.KEY_TARGET: self.client_id,
                self.KEY_API_TYPE: "app",
                self.KEY_REFRESH_TOKEN: self.refresh_token,
            }
        else:
            params = {
                self.KEY_CLIENT_ID: self.client_id,
                self.KEY_GRANT_TYPE: "refresh_token",
                self.KEY_REFRESH_TOKEN: self.refresh_token,
            }

            if self.scope is not None:
                params[self.KEY_SCOPE] = " ".join(self.scope)

        r = self.session.post(self.domain + "/token", json=params, timeout=timeout)

        if r.status_code != 200:
            raise OauthError("Could not retrieve token: {}".format(r.text.strip()))

        data = r.json()
        access_token = data.get("access_token")
        id_token = data.get("id_token")  # TODO(justin) remove legacy id_token usage

        if access_token is not None:
            self._token = access_token
        elif id_token is not None:
            self._token = id_token
        else:
            raise OauthError("Could not retrieve token")

        token_info = {}

        # Read the token from the token_info_path, and save it again
        if self.token_info_path:
            token_info = self._read_token_info(
                self.token_info_path, suppress_warning=True
            )

            if (
                token_info.get(self.KEY_CLIENT_ID) != self.client_id
                or token_info.get(self.KEY_CLIENT_SECRET) != self.client_secret
            ):
                # Not matching; better rewrite!
                token_info = {
                    self.KEY_CLIENT_ID: self.client_id,
                    self.KEY_CLIENT_SECRET: self.client_secret,
                    self.KEY_REFRESH_TOKEN: self.refresh_token,
                }

            token_info[self.KEY_JWT_TOKEN] = self._token
            token_info.pop(self.KEY_ALT_JWT_TOKEN, None)  # Remove alt key
            self._write_token_info(self.token_info_path, token_info)

    @property
    def namespace(self):
        """Gets the user namespace (the Descartes Labs used id).

        Returns
        -------
        str
            The user namespace.

        Raises
        ------
        AuthError
            Raised when incomplete credentials were provided.
        OauthError
            Raised when a token cannot be obtained or refreshed.
        """
        if self._namespace is None:
            self._namespace = sha1(self.payload["sub"].encode("utf-8")).hexdigest()
        return self._namespace

    def __getstate__(self):
        return dict((attr, getattr(self, attr)) for attr in self.__attrs__)

    def __setstate__(self, state):
        for name, value in state.items():
            setattr(self, name, value)

        self._init_adapter()
        self._init_session()


if __name__ == "__main__":
    auth = Auth.get_default_auth()

    print(auth.token)
