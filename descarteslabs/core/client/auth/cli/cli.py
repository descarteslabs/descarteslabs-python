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


import binascii
import json
import os

import click

from descarteslabs.auth.auth import (
    DEFAULT_TOKEN_INFO_PATH,
    DESCARTESLABS_CLIENT_ID,
    DESCARTESLABS_CLIENT_SECRET,
    DESCARTESLABS_REFRESH_TOKEN,
    DESCARTESLABS_TOKEN_INFO_PATH,
    Auth,
    base64url_decode,
    get_default_domain as get_auth_domain,
)


LOGIN_URL = f"{get_auth_domain()}/auth/refresh_token"


# this is defined this way to support mocking in the tests
# def get_default_domain():
#     from descarteslabs.auth.auth import get_default_domain as get_auth_domain

#     return get_auth_domain()


@click.group()
def cli():
    pass


@cli.command()
def login():
    """Log in to Descartes Labs"""
    click.echo(f"Follow this link to login:\n\n    {LOGIN_URL}\n")

    while True:
        try:
            s = input("...then come back here and paste the generated token: ")

            if not s:
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            click.echo("\nExiting without logging in")
            break

        if isinstance(s, str):
            s = s.encode("utf-8")

        if s:
            retry_message = f"""
You entered the wrong token. Please go to

{LOGIN_URL}

to retrieve your token"""

            try:
                json_data = base64url_decode(s).decode("utf-8")
            except (UnicodeDecodeError, binascii.Error):
                click.echo(retry_message)
                continue

            try:
                token_info = json.loads(json_data)
            except (UnicodeDecodeError, json.JSONDecodeError):
                click.echo(retry_message)
                continue

            if Auth.KEY_REFRESH_TOKEN not in token_info:
                token_info[Auth.KEY_REFRESH_TOKEN] = token_info.get(
                    Auth.KEY_CLIENT_SECRET
                )

            Auth._write_token_info(
                os.environ.get(DESCARTESLABS_TOKEN_INFO_PATH, DEFAULT_TOKEN_INFO_PATH),
                token_info,
            )

            # Get a fresh Auth token
            auth = Auth()
            name = auth.payload["name"]
            click.echo(f"Welcome, {name}!")
            break


@cli.command()
def payload():
    """Print the current token payload."""
    click.echo(json.dumps(Auth().payload, sort_keys=True, indent=4))


@cli.command()
def token():
    """Print the current token."""
    click.echo(Auth().token)


@cli.command()
def name():
    """Print the name of the current user."""
    click.echo(Auth().payload.get("name", ""))


@cli.command()
def groups():
    """Print the groups of the current user."""
    click.echo(json.dumps(sorted(Auth().payload["groups"]), indent=4))


@cli.command()
def env():
    """Print the environment settings for the current user."""
    click.echo(f"{DESCARTESLABS_CLIENT_ID}={Auth().client_id}")
    click.echo(f"{DESCARTESLABS_CLIENT_SECRET}={Auth().client_secret}")
    click.echo(f"{DESCARTESLABS_REFRESH_TOKEN}={Auth().refresh_token}")
