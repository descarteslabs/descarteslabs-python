import os
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

from descarteslabs.auth import Auth
from descarteslabs.exceptions import AuthError, ConfigError
from .. import Settings


# prevent any actual setup of `descarteslabs`
@patch("descarteslabs.config._aws_init._setup_aws", lambda: None)
@patch("descarteslabs.config._gcp_init._setup_gcp", lambda: None)
class TestSettings(unittest.TestCase):
    def setUp(self):
        # Clean up from any other tests
        Settings._settings = None

    @classmethod
    def tearDownClass(cls):
        # Clean up for tests following this suite
        Settings._settings = None

    def test_select_env_default(self):
        settings = Settings.select_env()
        self.assertEqual(settings.current_env, os.environ.get("DESCARTESLABS_ENV"))
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    @patch.dict(os.environ, {"DESCARTESLABS_ENV": "gcp-production"})
    def test_select_env_from_env(self):
        settings = Settings.select_env()
        self.assertEqual(settings.current_env, "gcp-production")
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    # environment must be patched because select_env will alter it
    @patch.dict(os.environ, clear=True)
    def test_select_env_from_string(self):
        settings = Settings.select_env("gcp-production")
        self.assertEqual(settings.current_env, "gcp-production")
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    def test_select_env_from_settings_file(self):
        settings = Settings.select_env(
            settings_file=os.path.join(os.path.dirname(__file__), "settings.toml")
        )
        self.assertEqual(settings.current_env, os.environ.get("DESCARTESLABS_ENV"))
        self.assertEqual(settings.testing, "hello")
        self.assertEqual(
            settings.gcp_client, os.environ.get("DESCARTESLABS_ENV") == "testing"
        )
        self.assertEqual(
            settings.aws_client, os.environ.get("DESCARTESLABS_ENV") == "aws-testing"
        )

    # environment must be patched because select_env will alter it
    @patch("descarteslabs.config.Auth")
    @patch.dict(os.environ, clear=True)
    def test_select_env_from_default_auth(self, mock_auth):
        settings = Settings.select_env()
        self.assertEqual(settings.current_env, "gcp-production")
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    # environment must be patched because select_env will alter it
    @patch("descarteslabs.config.Auth")
    @patch.dict(os.environ, clear=True)
    def test_select_env_from_no_auth(self, mock_auth):
        instance = MagicMock()
        type(instance).payload = PropertyMock(side_effect=AuthError())
        mock_auth.return_value = instance

        with self.assertRaises(ConfigError):
            Settings.select_env()

    # environment must be patched because select_env will alter it
    @patch.dict(os.environ, clear=True)
    def test_select_env_from_auth(self):
        auth = MagicMock(payload={"groups": ["aws-customer"]})
        settings = Settings.select_env(auth=auth)
        self.assertEqual(settings.current_env, "aws-production")
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    @patch.dict(os.environ, {"DESCARTESLABS_TESTING": "hello"})
    def test_select_env_override_from_env(self):
        settings = Settings.select_env()
        self.assertEqual(settings.current_env, os.environ.get("DESCARTESLABS_ENV"))
        self.assertEqual(settings.testing, "hello")

    @patch.dict(os.environ, {"DL_ENV": "testing", "DL_TESTING": "hello"})
    def test_select_env_prefix(self):
        settings = Settings.select_env(envvar_prefix="DL")
        self.assertEqual(settings.current_env, "testing")
        self.assertEqual(settings.testing, "hello")

    def test_get_settings(self):
        settings = Settings.get_settings()
        self.assertEqual(settings.current_env, os.environ.get("DESCARTESLABS_ENV"))
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    def test_peek_settings(self):
        env = "aws-testing"
        assert os.environ["DESCARTESLABS_ENV"] == "testing"
        settings = Settings.peek_settings(env)
        assert os.environ["DESCARTESLABS_ENV"] == "testing"
        assert settings.env == env
        assert Settings._settings is None

    def test_bad_env(self):
        env = "non-existent"

        with self.assertRaises(ConfigError):
            Settings.peek_settings(env)

        with self.assertRaises(ConfigError):
            Settings.select_env(env)

    def test_default_auth(self):
        a = Auth()
        a.domain == "http://gcp_url"

    def test_auth_with_env(self):
        with patch.dict(os.environ, {"DESCARTESLABS_ENV": "aws-testing"}):
            a = Auth()
            a.domain == "http://aws_url"

    def test_auth_with_aws_config(self):
        Settings.select_env("aws-testing")
        a = Auth()
        a.domain == "http://aws_url"

    def test_auth_with_gcp_config(self):
        Settings.select_env("testing")
        a = Auth()
        a.domain == "http://gcp_url"
