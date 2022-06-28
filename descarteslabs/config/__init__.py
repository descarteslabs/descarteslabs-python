import importlib
import os
import sys
from threading import Lock

import descarteslabs
import dynaconf
from descarteslabs.auth import Auth
from descarteslabs.exceptions import AuthError


GCP_ENVIRONMENT = "gcp-production"  #: Standard GCP environment
AWS_ENVIRONMENT = "aws-production"  #: Standard AWS environment


class Settings(dynaconf.Dynaconf):
    """
    Configuration settings for the descarteslabs client.

    Based on the ``dynaconf`` package. This settings class supports configuration from
    named "environments" in a ``settings.toml`` file as well as environment variables
    with names that are prefixed with ``DESCARTESLABS_`` (or whatever was specified
    in the `envvar_prefix`).

    For the full capabilities of ``dynaconf`` please consult https://www.dynaconf.com/.

    Note that normally ``Settings`` functions entirely automatically within the client.
    However, it is possible to perform custom initialization programmatically. In order
    to do this, the beginning of the client program must execute code like this:
    ```
    from descarteslabs.config import Settings
    Settings.select_env(...)
    ```
    before importing or otherwise accessing anything else within the descarteslabs
    package.
    """

    # the global settings instance, can only be set once via select_env
    _settings = None

    _lock = Lock()

    @classmethod
    def select_env(
        cls, env=None, settings_file=None, envvar_prefix="DESCARTESLABS", auth=None
    ):
        """
        Configure the descarteslabs client.

        Parameters
        ----------
        env : str, optional
            Name of the environment to configure. Must appear in
            ``descarteslabs/config/settings.toml`` If not supplied will be determined
            from the `DESCARTESLABS_ENV` environment variable (or whatever was specified
            in the `envvar_prefix`_ENV), if set, otherwise from the user's authenticated
            permissions.
        settings_file: str, optional
            If supplied, will be consulted for additional configuration overrides. These
            are applied over those in the ``descarteslabs/config/settings.toml`` file,
            but are themselves overwritten by any environment variable settings matching
            the `envvar_prefix`.
        envvar_prefix: str, optional
            Prefix for environment variable names to consult for configuration
            overrides. Environment variables with a leading prefix of
            ``"<envvar_prefix>_"`` will override the settings in the resulting
            configuration after the settings file(s) have been consulted.
        auth: Auth, optional
            If env is not supplied, then consult the user's authenticated permissions
            using this ``Auth`` instance. If not supplied, then a default ``Auth``
            instance is constructed.

        Returns
        -------
        Returns a ``Settings`` object, essentially a glorified dict-like object
        containing the configured settings for the client.
        """
        # once the settings has been lazy evaluated, we cannot change it.
        # the reviled double-check pattern. actually ok with CPython and the GIL, but not
        # necessarily any other Python implementation
        settings = cls._settings

        if settings is None:
            with cls._lock:
                settings = cls._settings

                if settings is None:
                    settings = cls._select_env(
                        env=env,
                        settings_file=settings_file,
                        envvar_prefix=envvar_prefix,
                        auth=auth,
                    )

        if settings is not None and env is not None and env != settings.ENV:
            raise RuntimeError(
                f"configuration environment already selected: {settings.ENV}"
            )

        return settings

    @classmethod
    def _select_env(
        cls, env=None, settings_file=None, envvar_prefix="DESCARTESLABS", auth=None
    ):
        selector = f"{envvar_prefix}_ENV"
        original_selector_value = os.environ.get(selector)

        if env:
            os.environ[selector] = env
        elif not os.environ.get(selector):
            # default it
            if auth is None:
                auth = Auth(_suppress_warning=True)

            try:
                groups = auth.payload.get("groups", {})
            except AuthError:
                raise RuntimeError(
                    "You need to login using `descarteslabs auth login` in order to proceed."
                )

            if "aws-customer" in groups:
                os.environ[selector] = "aws-production"
            else:
                os.environ[selector] = "gcp-production"

        builtin_settings_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "settings.toml"
        )

        settings = cls(
            settings_file=[builtin_settings_file],
            includes=[] if not settings_file else [settings_file],
            # disable other file loaders since we only use toml
            # env_loader is enabled by default, and is not disabled here
            core_loaders=["TOML"],
            # allow loading all environments, choosing the
            # active environment from DESCARTESLABS_ENV
            environments=True,
            # don't use default values to avoid mishaps
            # determines which environment variable specifies
            # the active environment
            env_switcher=selector,
            # the prefix to envvar overrides, e.g DESCARTESLABS_MY_SETTING
            envvar_prefix=envvar_prefix,
            # if environment can't load, then fail hard
            silent_errors_for_dynaconf=False,
        )

        # Make sure we selected an environment!
        try:
            # Checking whether ENV is present doesn't work...
            assert settings.env
            assert settings.gcp_client or settings.aws_client
        except (AttributeError, KeyError, AssertionError):
            env = os.environ[selector]

            if original_selector_value is None:
                os.environ.pop(selector)
            else:
                os.environ[selector] = original_selector_value

            raise KeyError(f"{env}: That environment doesn't exist!") from None

        # assign to the global instance
        cls._settings = settings

        if os.path.exists(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "client")
        ):
            # we are in monorepo, don't configure any client (currently this means we do not
            # support the AWS client within monorepo)
            pass
        else:
            # we are in a client install
            # first remove the fake descarteslabs module
            # note that as we are always imported by `descarteslabs` before the fake
            # module is installed, our `descarteslabs` attribute here is the real
            # module.
            sys.modules[descarteslabs.__name__] = descarteslabs

            # super important! We hold the Settings._lock, so no (transitive)
            # imports can try to use `get_settings()`!
            if settings.aws_client:
                from ._aws_init import _setup_aws

                _setup_aws()
            elif settings.gcp_client:
                from ._gcp_init import _setup_gcp

                _setup_gcp()

            # invalidate the meta_path caches
            importlib.invalidate_caches()

        # and return the settings object
        return settings

    @classmethod
    def get_settings(cls):
        """
        Retrieve the current ``Settings`` for the client.
        """
        # the reviled double-check pattern. actually ok with CPython and the GIL, but not
        # necessarily any other Python implementation
        settings = cls._settings
        if settings is None:
            with cls._lock:
                settings = cls._settings
                if settings is None:
                    settings = cls._select_env()
        return settings


get_settings = Settings.get_settings
select_env = Settings.select_env

__all__ = [
    "AWS_ENVIRONMENT",
    "GCP_ENVIRONMENT",
    "Settings",
    "get_settings",
    "select_env",
]
