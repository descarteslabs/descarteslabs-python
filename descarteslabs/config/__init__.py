import importlib
import os
import sys
from threading import Lock

import dynaconf

import descarteslabs
from descarteslabs.auth import Auth
from descarteslabs.exceptions import AuthError, ConfigError

GCP_ENVIRONMENT = "gcp-production"  #: Standard GCP environment
AWS_ENVIRONMENT = "aws-production"  #: Standard AWS environment


class Settings(dynaconf.Dynaconf):
    """
    Configuration settings for the Descartes Labs client.

    Based on the ``Dynaconf`` package. This settings class supports configuration from
    named "environments" in a ``settings.toml`` file as well as environment variables
    with names that are prefixed with ``DESCARTESLABS_`` (or the prefix specified
    in the `envvar_prefix`).

    For the full capabilities of ``Dynaconf`` please consult https://www.dynaconf.com/.

    Note that normally ``Settings`` functions entirely automatically within the client.
    However, it is possible to perform custom initialization programmatically. In order
    to do this, the beginning of the client program must execute code like this:

    .. code-block::

        from descarteslabs.config import Settings
        Settings.select_env(...)

    Before importing or otherwise accessing anything else within the
    :py:mod:`descarteslabs` package.
    """

    class _EnvDescriptor:
        # Retrieve the correct env string for `peek_settings()`
        def __get__(self, obj, objtype=None):
            if obj is None:
                if objtype._settings is None:
                    return None
                else:
                    return objtype._settings.env_for_dynaconf
            else:
                return obj.env_for_dynaconf

    env = _EnvDescriptor()
    """str : The current client configuration name or `None` of no environment was selected."""

    # The global settings instance, can only be set once via select_env or get_settings
    _settings = None

    _lock = Lock()

    @classmethod
    def select_env(
        cls, env=None, settings_file=None, envvar_prefix="DESCARTESLABS", auth=None
    ):
        """
        Configure the Descartes Labs client.

        Parameters
        ----------
        env : str, optional
            Name of the environment to configure. Must appear in
            ``descarteslabs/config/settings.toml`` If not supplied will be determined
            from the `DESCARTESLABS_ENV` environment variable (or use the prefix
            specified in the `envvar_prefix`_ENV), if set, otherwise from the user's
            authenticated permissions.
        settings_file : str, optional
            If supplied, will be consulted for additional configuration overrides. These
            are applied over those in the ``descarteslabs/config/settings.toml`` file,
            but are themselves overwritten by any environment variable settings matching
            the `envvar_prefix`.
        envvar_prefix : str, optional
            Prefix for environment variable names to consult for configuration
            overrides. Environment variables with a leading prefix of
            ``"<envvar_prefix>_"`` will override the settings in the resulting
            configuration after the settings file(s) have been consulted.
        auth : Auth, optional
            If ``env`` is not supplied, then consult the user's authenticated
            permissions using this ``Auth`` instance. If not supplied, then a
            default ``Auth`` instance is constructed.

        Returns
        -------
        Settings
            Returns a ``Settings`` instance, a dict-like object
            containing the configured settings for the client.

        Raises
        ------
        ConfigError
            If no client configuration could be established, or if an invalid
            configuration name was specified, or if you try to change the
            client configuration after the client is already configured.
        """
        # Once the settings has been lazy evaluated, we cannot change it.
        # The reviled double-check pattern. Actually ok with CPython and the GIL,
        # but not necessarily any other Python implementation.
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
            raise ConfigError(
                f"Client configuration '{settings.ENV}' has already been selected"
            )

        return settings

    @classmethod
    def get_settings(cls):
        """
        Configure and retrieve the current or default settings for the client.

        Returns
        -------
        Settings
            Returns a ``Settings`` instance, a dict-like object
            containing the configured settings for the client.

        Raises
        ------
        ConfigError
            If no client configuration could be established, or if an invalid
            configuration name was specified, or if you try to change the
            client configuration after the client is already configured.
        """
        # The reviled double-check pattern. Actually ok with CPython and the GIL,
        # but not necessarily any other Python implementation.
        settings = cls._settings

        if settings is None:
            with cls._lock:
                settings = cls._settings
                if settings is None:
                    settings = cls._select_env()

        return settings

    @classmethod
    def peek_settings(
        cls, env=None, settings_file=None, envvar_prefix="DESCARTESLABS", auth=None
    ):
        """Retrieve the settings without configuring the client.

        Unlike :py:meth:`~Settings.get_settings` and :py:meth:`~Settings.select_env`
        which both will configure the client, the :py:meth:`~Settings.peek_settings`
        will not configure the client and :py:attr:`Settings.env` will not be set.

        See :py:meth:`select_env` for an explanation of the parameters, return value,
        and exceptions that can be raised.
        """
        selector = f"{envvar_prefix}_ENV"
        original_selector_value = os.environ.get(selector)

        settings = cls._get_settings(
            env=env,
            settings_file=settings_file,
            envvar_prefix=envvar_prefix,
            auth=auth,
        )

        # Return the environ back to its original state
        if original_selector_value is None:
            os.environ.pop(selector)
        else:
            os.environ[selector] = original_selector_value

        return settings

    @classmethod
    def _select_env(
        cls, env=None, settings_file=None, envvar_prefix="DESCARTESLABS", auth=None
    ):
        # Assign to the global instance.
        cls._settings = cls._get_settings(
            env=env, settings_file=settings_file, envvar_prefix=envvar_prefix, auth=auth
        )

        if os.path.exists(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "client")
        ):
            # We are in monorepo, don't configure any client (currently this means we
            # do not support the AWS client within monorepo).
            pass
        else:
            # We are in a client install.
            # First remove the fake descarteslabs module.
            # Note that as we are always imported by `descarteslabs` before the fake
            # module is installed, our `descarteslabs` attribute here is the real
            # module.
            sys.modules[descarteslabs.__name__] = descarteslabs

            # Super important! We hold the Settings._lock, so no (transitive)
            # imports can try to use `get_settings()`!
            if cls._settings.aws_client:
                from ._aws_init import _setup_aws

                _setup_aws()
            elif cls._settings.gcp_client:
                from ._gcp_init import _setup_gcp

                _setup_gcp()

            # Invalidate the meta_path caches.
            importlib.invalidate_caches()

        # And return the settings object.
        return cls._settings

    @classmethod
    def _get_settings(
        cls, env=None, settings_file=None, envvar_prefix="DESCARTESLABS", auth=None
    ):
        # Get the settings. If the settings are retrieved successfully, the os.environ
        # will contain the selector for the given settings.
        selector = f"{envvar_prefix}_ENV"
        original_selector_value = os.environ.get(selector)

        def restore_env():
            if original_selector_value is None:
                os.environ.pop(selector)
            else:
                os.environ[selector] = original_selector_value

        if env:
            os.environ[selector] = env
        elif not os.environ.get(selector):
            # Default it.
            if auth is None:
                auth = Auth(_suppress_warning=True)

            try:
                groups = auth.payload.get("groups", {})
            except AuthError:
                raise ConfigError(
                    "Cannot determine your client configuration. Either set the "
                    "DESCARTESLABS_ENV environment variable to the desired "
                    "environment or login using 'descarteslabs auth login' in "
                    "order to proceed. See "
                    "https://docs.descarteslabs.com/configuration.html#environments "
                    "for more information."
                )

            if "aws-customer" in groups:
                os.environ[selector] = "aws-production"
            else:
                os.environ[selector] = "gcp-production"

        builtin_settings_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "settings.toml"
        )

        try:
            # By default this will load from one or more settings files and
            # then from the environment.
            settings = cls(
                # First load our client settings from TOML file.
                settings_file=[builtin_settings_file],
                # Then load the given settings from TOML file, if any.
                includes=[] if not settings_file else [settings_file],
                # Only allow TOML format.
                core_loaders=["TOML"],
                # Allow multiple environments ([default] is always used).
                environments=True,
                # Name of environment variable that selects the environment.
                env_switcher=selector,
                # Prefix to overwrite loaded settings, e.g,. {envvar_prefix}_MY_SETTING.
                envvar_prefix=envvar_prefix,
            )
        except Exception as e:
            restore_env()
            raise ConfigError(str(e)) from e

        try:
            # Make sure we selected an environment!
            assert settings.env_for_dynaconf
            assert settings.gcp_client or settings.aws_client
        except (AttributeError, KeyError, AssertionError):
            message = f"Client configuration '{os.environ[selector]}' doesn't exist!"
            restore_env()

            if not env:
                message += " Check your DESCARTESLABS_ENV environment variable."

            raise ConfigError(message) from None

        return settings


get_settings = Settings.get_settings
"""An alias for :py:meth:`Settings.get_settings`"""

peek_settings = Settings.peek_settings
"""An alias for :py:meth:`Settings.peek_settings`"""

select_env = Settings.select_env
"""An alias for :py:meth:`Settings.select_env`"""

__all__ = [
    "AWS_ENVIRONMENT",
    "GCP_ENVIRONMENT",
    "Settings",
    "get_settings",
    "peek_settings",
    "select_env",
]
