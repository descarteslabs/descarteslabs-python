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


import os
import json
import binascii

from pprint import pprint

from descarteslabs.auth.auth import (
    Auth,
    base64url_decode,
    DEFAULT_TOKEN_INFO_PATH,
    DESCARTESLABS_TOKEN_INFO_PATH,
    DESCARTESLABS_CLIENT_ID,
    DESCARTESLABS_CLIENT_SECRET,
    DESCARTESLABS_REFRESH_TOKEN,
)
from ..version import __version__


LOGIN_URL = "https://iam.descarteslabs.com/auth/refresh_token"


def auth_handler(args):
    auth = Auth(_suppress_warning=True)

    if args.command == "login":
        print(f"Follow this link to login:\n\n    {LOGIN_URL}\n")

        while True:
            try:
                s = input("...then come back here and paste the generated token: ")

                if not s:
                    raise KeyboardInterrupt()
            except KeyboardInterrupt:
                print("\nExiting without logging in")
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
                    print(retry_message)
                    continue

                try:
                    token_info = json.loads(json_data)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    print(retry_message)
                    continue

                if (
                    Auth.KEY_REFRESH_TOKEN not in token_info
                ):  # TODO(justin) legacy for previous IDP
                    token_info[Auth.KEY_REFRESH_TOKEN] = token_info.get(
                        Auth.KEY_CLIENT_SECRET
                    )

                Auth._write_token_info(
                    os.environ.get(
                        DESCARTESLABS_TOKEN_INFO_PATH, DEFAULT_TOKEN_INFO_PATH
                    ),
                    token_info,
                )

                # Get a fresh Auth token
                auth = Auth()
                name = auth.payload["name"]
                print(f"Welcome, {name}!")
                break

    if args.command == "token":
        print(auth.token)

    if args.command == "name":
        auth.token
        print(auth.payload["name"])

    if args.command == "groups":
        auth.token
        print(json.dumps(auth.payload["groups"]))

    if args.command == "payload":
        auth.token
        pprint(auth.payload)

    if args.command == "env":
        auth.token
        print(f"{DESCARTESLABS_CLIENT_ID}={auth.client_id}")
        print(f"{DESCARTESLABS_CLIENT_SECRET}={auth.client_secret}")
        print(f"{DESCARTESLABS_REFRESH_TOKEN}={auth.refresh_token}")

    if args.command == "version":
        print(__version__)
