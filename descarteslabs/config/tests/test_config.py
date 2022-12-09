import os
import unittest
from copy import deepcopy
from unittest.mock import patch, MagicMock, PropertyMock

from descarteslabs.auth import Auth
from descarteslabs.exceptions import AuthError, ConfigError
from .. import Settings


# prevent any actual setup of `descarteslabs`
@patch("descarteslabs.config._aws_init._setup_aws", lambda: None)
@patch("descarteslabs.config._gcp_init._setup_gcp", lambda: None)
class TestSettings(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Save settings and environment
        cls.settings = Settings._settings
        cls.environ = deepcopy(os.environ)

    def setUp(self):
        # Clear existing settings from test environment
        Settings._settings = None

    def tearDown(self):
        # Restore settings and environment
        Settings._settings = self.settings
        os.environ.clear()
        os.environ.update(self.environ)

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
            settings_file=os.path.join(os.path.dirname(__file__), "settings.toml"),
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
        current_env = os.environ["DESCARTESLABS_ENV"]
        env = "aws-testing"
        settings = Settings.peek_settings(env)
        assert os.environ["DESCARTESLABS_ENV"] == current_env
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

    def test_env(self):
        peek1_env = "aws-dev"
        env = "aws-staging"

        assert Settings.env is None
        s1 = Settings.peek_settings(peek1_env)
        assert s1.env == peek1_env
        assert Settings.env is None

        s2 = Settings.select_env(env)
        assert s2.env == env
        assert s1.env == peek1_env
        assert Settings.env == env

        peek2_env = "gcp-stage"
        s3 = Settings.peek_settings(peek2_env)
        assert s3.env == peek2_env
        assert s2.env == env
        assert s1.env == peek1_env
        assert Settings.env == env


class VerifyValues(unittest.TestCase):
    configs = {
        "aws-dev": {
            "AWS_CLIENT": True,
            "CATALOG_V2_URL": "https://platform.dev.aws.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": False,
            "IAM_URL": "https://iam.dev.aws.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.dev.aws.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.dev.aws.descarteslabs.com",
            "RASTER_URL": "https://platform.dev.aws.descarteslabs.com/raster/v2",
            "USAGE_URL": "https://platform.dev.aws.descarteslabs.com/usage/v1",
            "YAAS_URL": "https://platform.dev.aws.descarteslabs.com/yaas/v1",
        },
        "aws-production": {
            "AWS_CLIENT": True,
            "CATALOG_V2_URL": "https://platform.production.aws.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": False,
            "IAM_URL": "https://iam.production.aws.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.production.aws.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.production.aws.descarteslabs.com",
            "RASTER_URL": "https://platform.production.aws.descarteslabs.com/raster/v2",
            "USAGE_URL": "https://platform.production.aws.descarteslabs.com/usage/v1",
            "YAAS_URL": "https://platform.production.aws.descarteslabs.com/yaas/v1",
        },
        "aws-staging": {
            "AWS_CLIENT": True,
            "CATALOG_V2_URL": "https://platform.staging.aws.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": False,
            "IAM_URL": "https://iam.staging.aws.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.staging.aws.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.staging.aws.descarteslabs.com",
            "RASTER_URL": "https://platform.staging.aws.descarteslabs.com/raster/v2",
            "USAGE_URL": "https://platform.staging.aws.descarteslabs.com/usage/v1",
            "YAAS_URL": "https://platform.staging.aws.descarteslabs.com/yaas/v1",
        },
        "aws-testing": {
            "AWS_CLIENT": True,
            "CATALOG_V2_URL": "https://platform.dev.aws.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": False,
            "IAM_URL": "https://iam.dev.aws.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.dev.aws.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.dev.aws.descarteslabs.com",
            "RASTER_URL": "https://platform.dev.aws.descarteslabs.com/raster/v2",
            "TESTING": True,
            "USAGE_URL": "https://platform.dev.aws.descarteslabs.com/usage/v1",
            "YAAS_URL": "https://platform.dev.aws.descarteslabs.com/yaas/v1",
        },
        "gcp-compute-production": {
            "AWS_CLIENT": False,
            "GCP_CLIENT": True,
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.descarteslabs.com",
            "RASTER_URL": "http://ruster.ruster:8000",
            "WORKFLOWS_HOST": "workflows-api.prod.descarteslabs.com",
            "WORKFLOWS_HOST_HTTP": "workflows.prod.descarteslabs.com",
            "WORKFLOWS_PORT": "443",
        },
        "gcp-compute-stage": {
            "AWS_CLIENT": False,
            "GCP_CLIENT": True,
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.descarteslabs.com",
            "RASTER_URL": "http://ruster.ruster:8000",
            "WORKFLOWS_HOST": "workflows-api.stage.descarteslabs.com",
            "WORKFLOWS_HOST_HTTP": "workflows.stage.descarteslabs.com",
            "WORKFLOWS_PORT": "443",
        },
        "gcp-prerelease": {
            "ANNOTATION_URL": "https://annotation-prerelease.descarteslabs.com",
            "AWS_CLIENT": False,
            "CATALOG_URL": "https://platform-prerelease.descarteslabs.com/metadata/v1/catalog",
            "CATALOG_V2_URL": "https://platform-prerelease.descarteslabs.com/metadata/v1/catalog/v2",
            "CURRIER_HOST": "platform-prerelease.descarteslabs.com",
            "CURRIER_PORT": "443",
            "DISCOVER_HOST": "platform-prerelease.descarteslabs.com",
            "DISCOVER_PORT": "443",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform-prerelease.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam-prerelease.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform-prerelease.descarteslabs.com/metadata/v1",
            "PLACES_URL": "https://platform.descarteslabs.com/waldo/v2",
            "PLATFORM_URL": "https://platform-prerelease.descarteslabs.com",
            "RASTER_URL": "https://platform-prerelease.descarteslabs.com/raster/v2",
            "SHARING_URL": "https://sharing.descarteslabs.com",
            "STORAGE_URL": "https://platform-prerelease.descarteslabs.com/storage/v1",
            "TABLES_HOST": "platform-prerelease.descarteslabs.com",
            "TABLES_PORT": "443",
            "TASKS_URL": "https://platform.descarteslabs.com/tasks/v2",
            "VECTOR_URL": "https://platform-prerelease.descarteslabs.com/vector/v2",
            "WORKFLOWS_HOST": "workflows-api.prod.descarteslabs.com",
            "WORKFLOWS_HOST_HTTP": "workflows.prod.descarteslabs.com",
            "WORKFLOWS_PORT": "443",
            "YAAS_URL": "https://platform-prerelease.descarteslabs.com/yaas/v1",
        },
        "gcp-production": {
            "ANNOTATION_URL": "https://annotation.descarteslabs.com",
            "AWS_CLIENT": False,
            "CATALOG_URL": "https://platform.descarteslabs.com/metadata/v1/catalog",
            "CATALOG_V2_URL": "https://platform.descarteslabs.com/metadata/v1/catalog/v2",
            "CURRIER_HOST": "platform.descarteslabs.com",
            "CURRIER_PORT": "443",
            "DISCOVER_HOST": "platform.descarteslabs.com",
            "DISCOVER_PORT": "443",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.descarteslabs.com/metadata/v1",
            "PLACES_URL": "https://platform.descarteslabs.com/waldo/v2",
            "PLATFORM_URL": "https://platform.descarteslabs.com",
            "RASTER_URL": "https://platform.descarteslabs.com/raster/v2",
            "SHARING_URL": "https://sharing.descarteslabs.com",
            "STORAGE_URL": "https://platform.descarteslabs.com/storage/v1",
            "TABLES_HOST": "platform.descarteslabs.com",
            "TABLES_PORT": "443",
            "TASKS_URL": "https://platform.descarteslabs.com/tasks/v1",
            "VECTOR_URL": "https://platform.descarteslabs.com/vector/v2",
            "WORKFLOWS_HOST": "workflows-api.prod.descarteslabs.com",
            "WORKFLOWS_HOST_HTTP": "workflows.prod.descarteslabs.com",
            "WORKFLOWS_PORT": "443",
            "YAAS_URL": "https://platform.descarteslabs.com/yaas/v1",
        },
        "gcp-stage": {
            "ANNOTATION_URL": "https://annotation.stage.descarteslabs.com",
            "AWS_CLIENT": False,
            "CATALOG_URL": "https://platform.stage.descarteslabs.com/metadata/v1/catalog",
            "CATALOG_V2_URL": "https://platform.stage.descarteslabs.com/metadata/v1/catalog/v2",
            "CURRIER_HOST": "platform.stage.descarteslabs.com",
            "CURRIER_PORT": "443",
            "DISCOVER_HOST": "platform.stage.descarteslabs.com",
            "DISCOVER_PORT": "443",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform.stage.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam.stage.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.stage.descarteslabs.com/metadata/v1",
            "PLACES_URL": "https://platform.descarteslabs.com/waldo/v2",
            "PLATFORM_URL": "https://platform.stage.descarteslabs.com",
            "RASTER_URL": "https://platform.stage.descarteslabs.com/raster/v2",
            "SHARING_URL": "https://sharing.descarteslabs.com",
            "STORAGE_URL": "https://platform.stage.descarteslabs.com/storage/v1",
            "TABLES_HOST": "platform.stage.descarteslabs.com",
            "TABLES_PORT": "443",
            "TASKS_URL": "https://platform.descarteslabs.com/tasks/default",
            "VECTOR_URL": "https://platform.stage.descarteslabs.com/vector/v2",
            "WORKFLOWS_HOST": "workflows-api.stage.descarteslabs.com",
            "WORKFLOWS_HOST_HTTP": "workflows.stage.descarteslabs.com",
            "WORKFLOWS_PORT": "443",
            "YAAS_URL": "https://platform.stage.descarteslabs.com/yaas/v1",
        },
        "testing": {
            "ANNOTATION_URL": "https://annotation-prerelease.descarteslabs.com",
            "AWS_CLIENT": False,
            "CATALOG_URL": "https://platform.descarteslabs.com/metadata/v1/catalog",
            "CATALOG_V2_URL": "https://platform.descarteslabs.com/metadata/v1/catalog/v2",
            "CURRIER_HOST": "platform.descarteslabs.com",
            "CURRIER_PORT": "443",
            "DISCOVER_HOST": "platform.descarteslabs.com",
            "DISCOVER_PORT": "443",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.descarteslabs.com/metadata/v1",
            "PLACES_URL": "https://platform.descarteslabs.com/waldo/v2",
            "PLATFORM_URL": "https://platform.descarteslabs.com",
            "RASTER_URL": "https://platform.descarteslabs.com/raster/v2",
            "SHARING_URL": "https://sharing.descarteslabs.com",
            "STORAGE_URL": "https://platform.descarteslabs.com/storage/v1",
            "TABLES_HOST": "platform.descarteslabs.com",
            "TABLES_PORT": "443",
            "TESTING": True,
            "TASKS_URL": "https://platform.descarteslabs.com/tasks/v1",
            "VECTOR_URL": "https://platform.descarteslabs.com/vector/v2",
            "WORKFLOWS_HOST": "workflows-api.prod.descarteslabs.com",
            "WORKFLOWS_HOST_HTTP": "workflows.prod.descarteslabs.com",
            "WORKFLOWS_PORT": "443",
            "USAGE_URL": "https://platform.descarteslabs.com/usage/v1",
            "YAAS_URL": "https://platform.descarteslabs.com/yaas/v1",
        },
    }

    def test_verify_configs(self):
        for config_name, config in self.configs.items():
            settings = Settings.peek_settings(config_name)

            for key in config.keys():
                assert (
                    config[key] == settings[key]
                ), f"{config_name}: {key}: {config[key]} != {settings[key]}"

    def test_verify_as_dict(self):
        for config_name, config in self.configs.items():
            settings = Settings.peek_settings(config_name)
            settings = settings.as_dict()

            for key in config.keys():
                assert (
                    config[key] == settings[key]
                ), f"{config_name}: {key}: {config[key]} != {settings[key]}"

    def test_verify_get(self):
        for config_name, config in self.configs.items():
            settings = Settings.peek_settings(config_name)

            for key in config.keys():
                value = settings.get(key)

                assert (
                    config[key] == value
                ), f"{config_name}: {key}: {config[key]} != {value}"

    def test_remaining_keys(self):
        for config_name, config in self.configs.items():
            settings = Settings.peek_settings(config_name)
            settings = settings.as_dict()

            for key in config.keys():
                settings.pop(key)

            settings.pop("DEFAULT_DOMAIN", None)  # Added since 1.11.0
            settings.pop("DEFAULT_HOST", None)  # Added since 1.11.0
            settings.pop("DOMAIN")  # Added since 1.11.0

            assert settings.pop("ENV") == config_name
            assert len(settings) == 0, f"{config_name}: {settings}"
