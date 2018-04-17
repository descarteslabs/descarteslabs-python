# Copyright 2018 Descartes Labs.
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
import stat
import json
import six
from six.moves import input

from descarteslabs.client.auth.auth import Auth, base64url_decode, makedirs_if_not_exists, DEFAULT_TOKEN_INFO_PATH
from descarteslabs.client.version import __version__


def auth_handler(args):
    auth = Auth()

    if args.command == 'login':

        print(
            'Follow this link to login https://iam.descarteslabs.com/auth/login?refresh_token=true&destination=/auth/refresh_token')   # NOQA

        s = input('...then come back here and paste the generated token: ')
        if isinstance(s, six.text_type):
            s = s.encode('utf-8')

        if s:

            token_info = json.loads(base64url_decode(s).decode('utf-8'))
            if "refresh_token" not in token_info:  # TODO(justin) legacy for previous IDP
                token_info['refresh_token'] = token_info.get("client_secret")

            token_info_directory = os.path.dirname(DEFAULT_TOKEN_INFO_PATH)
            makedirs_if_not_exists(token_info_directory)

            os.chmod(token_info_directory, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

            with open(DEFAULT_TOKEN_INFO_PATH, 'w+') as fp:
                json.dump(token_info, fp)

            os.chmod(DEFAULT_TOKEN_INFO_PATH, stat.S_IRUSR | stat.S_IWUSR)

            # Get a fresh Auth token
            auth = Auth()

            name = auth.payload['name']

            print('Welcome, %s!' % name)

    if args.command == 'token':
        print(auth.token)

    if args.command == 'name':
        auth.token
        print(auth.payload['name'])

    if args.command == 'groups':
        auth.token
        print(json.dumps(auth.payload['groups']))

    if args.command == 'payload':
        auth.token
        print(auth.payload)

    if args.command == 'env':
        auth.token
        print('%s=%s' % ('CLIENT_ID', auth.client_id))
        print('%s=%s' % ('CLIENT_SECRET', auth.client_secret))
        print('%s=%s' % ('REFRESH_TOKEN', auth.refresh_token))

    if args.command == 'version':
        print(__version__)
