# Copyright 2018-2023 Descartes Labs.
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

import os
import unittest
from copy import deepcopy
from unittest.mock import patch

from descarteslabs.auth import Auth
from descarteslabs.exceptions import ConfigError

from .. import Settings


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

    @patch.dict(os.environ, {"DESCARTESLABS_ENV": "aws-production"})
    def test_select_env_from_env(self):
        settings = Settings.select_env()
        self.assertEqual(settings.current_env, "aws-production")
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    # environment must be patched because select_env will alter it
    @patch.dict(os.environ, clear=True)
    def test_select_env_from_string(self):
        settings = Settings.select_env("aws-production")
        self.assertEqual(settings.current_env, "aws-production")
        self.assertEqual(id(settings), id(Settings._settings))
        self.assertEqual(id(settings), id(Settings.get_settings()))

    def test_select_env_from_settings_file(self):
        settings = Settings.select_env(
            settings_file=os.path.join(os.path.dirname(__file__), "settings.toml"),
        )
        self.assertEqual(settings.current_env, os.environ.get("DESCARTESLABS_ENV"))
        self.assertEqual(settings.testing, "hello")
        self.assertEqual(
            settings.aws_client, os.environ.get("DESCARTESLABS_ENV") == "testing"
        )
        self.assertEqual(
            settings.gcp_client, os.environ.get("DESCARTESLABS_ENV") == "gcp-testing"
        )

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
            "COMPUTE_URL": "https://platform.dev.aws.descarteslabs.com/compute/v1",
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
            "COMPUTE_URL": "https://platform.production.aws.descarteslabs.com/compute/v1",
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
            "COMPUTE_URL": "https://platform.staging.aws.descarteslabs.com/compute/v1",
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
            "COMPUTE_URL": "https://platform.dev.aws.descarteslabs.com/compute/v1",
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
        "gcp-prerelease": {
            "AWS_CLIENT": False,
            "CATALOG_V2_URL": "https://platform-prerelease.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform-prerelease.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam-prerelease.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform-prerelease.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform-prerelease.descarteslabs.com",
            "RASTER_URL": "https://platform-prerelease.descarteslabs.com/raster/v2",
            "YAAS_URL": "https://platform-prerelease.descarteslabs.com/yaas/v1",
        },
        "gcp-production": {
            "AWS_CLIENT": False,
            "CATALOG_V2_URL": "https://platform.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.descarteslabs.com",
            "RASTER_URL": "https://platform.descarteslabs.com/raster/v2",
            "YAAS_URL": "https://platform.descarteslabs.com/yaas/v1",
        },
        "gcp-stage": {
            "AWS_CLIENT": False,
            "CATALOG_V2_URL": "https://platform.stage.descarteslabs.com/metadata/v1/catalog/v2",
            "GCP_CLIENT": True,
            "GRPC_HOST": "platform.stage.descarteslabs.com",
            "GRPC_PORT": "443",
            "IAM_URL": "https://iam.stage.descarteslabs.com",
            "LOG_LEVEL": "WARNING",
            "METADATA_URL": "https://platform.stage.descarteslabs.com/metadata/v1",
            "PLATFORM_URL": "https://platform.stage.descarteslabs.com",
            "RASTER_URL": "https://platform.stage.descarteslabs.com/raster/v2",
            "YAAS_URL": "https://platform.stage.descarteslabs.com/yaas/v1",
        },
        "testing": {
            "AWS_CLIENT": True,
            "CATALOG_V2_URL": "https://platform.dev.aws.descarteslabs.com/metadata/v1/catalog/v2",
            "COMPUTE_URL": "https://platform.dev.aws.descarteslabs.com/compute/v1",
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
