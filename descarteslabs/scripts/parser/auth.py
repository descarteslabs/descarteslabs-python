# Copyright 2017 Descartes Labs.
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

from __future__ import print_function

import os
import stat
import json
import six
from six.moves import input

from descarteslabs.auth import Auth, base64url_decode

import descarteslabs as dl


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

            path = os.path.join(os.path.expanduser("~"), '.descarteslabs')

            if not os.path.exists(path):
                os.makedirs(path)

            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

            file = os.path.join(os.path.expanduser("~"), '.descarteslabs', 'token_info.json')

            with open(file, 'w+') as fp:
                json.dump(token_info, fp)

            os.chmod(file, stat.S_IRUSR | stat.S_IWUSR)

            # Get a fresh Auth token
            auth = dl.Auth()
            dl.metadata.auth = auth
            keys = dl.metadata.keys()

            name = auth.payload['name']
            groups = ', '.join(auth.payload['groups'])

            if len(keys):

                print('Welcome, %s!' % name)

            else:

                print('Welcome, %s! Your %s role(s) do not permit access to any imagery at this time.' % (name, groups))
                print(
                    'Contact support@descarteslabs.com if you believe you received this message in error or have any questions.')  # NOQA

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

    if args.command == 'version':
        print(dl.__version__)
