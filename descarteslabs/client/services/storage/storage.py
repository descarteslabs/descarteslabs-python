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
import warnings

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.service import Service, ThirdPartyService


class Storage(Service):
    """Data Storage Service"""

    TIMEOUT = (9.5, 120)

    def __init__(self, url=None, auth=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if auth is None:
            auth = Auth()

        warnings.simplefilter('always', DeprecationWarning)
        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_STORAGE_URL",
                "https://platform.descarteslabs.com/storage/v1"
            )

        self._gcs_upload_service = ThirdPartyService()

        super(Storage, self).__init__(url, auth=auth)

    def get_signed_url(self, key, storage_type='data'):
        r = self.session.get('/{storage_type}/get_signed_url/{key}'.format(storage_type=storage_type, key=key))
        r.raise_for_status()
        return r.content.decode('ascii')

    def get_upload_url(self, key, storage_type='data', **kwargs):
        r = self.session.get(
            '/{storage_type}/new_resumable_url/{key}'.format(storage_type=storage_type, key=key),
            params=kwargs
        )
        r.raise_for_status()
        return r.content.decode('ascii')

    def delete(self, key, storage_type='data'):
        """
        Delete the data stored at location `key` with storage type
        `storage_type`

        :param str key: A unique string mapped to an existing storage blob
        :param str storage_type: A type of data storage. Possible values: data", "tmp", "result".  Default: "data".

        """

        self.session.delete('/{storage_type}/{key}'.format(storage_type=storage_type, key=key))

    def set(self, key, value, storage_type='data'):
        """
        Store string `value` at location `key`, with storage type
        `storage_type`

        :param str key: A unique string mapped to an existing storage blob
        :param str value: bytes to be stored at location `key`
        :param str storage_type: A type of data storage. Possible values: "data", "tmp", "result".  Default: "data".
        """

        rurl = self.get_upload_url(key, storage_type=storage_type)

        self._gcs_upload_service.session.put(rurl, data=value)
        return

    def get(self, key, storage_type='data'):
        """
        Retrieve data stored at location `key`, with storage type
        `storage_type`

        :param str key: A unique string mapped to an existing storage blob
        :param str storage_type: A type of data storage. Possible values: "data", "tmp", "result".  Default: "data".

        Returns:
            The string that was stored at `key`

        """

        r = self.session.get('/{storage_type}/get/{key}'.format(storage_type=storage_type, key=key))
        r.raise_for_status()
        return r.content

    def list(self, prefix=None, storage_type='data'):
        """
        List keys that have been stored, with an optional `prefix` and `storage_type`.

        :param str prefix: A prefix match of keys returned.
        :param str storage_type: A type of data storage. Possible values: "data", "tmp", "result".  Default: "data".

        Returns:
            A list of keys.

        """

        r = self.session.get(
            '/{storage_type}/list'.format(storage_type=storage_type),
            params={'prefix': prefix}
        )
        r.raise_for_status()
        return r.json()

    def iter_list(self, prefix=None, storage_type='data'):
        """
        Yield keys that have been stored, with an optional `prefix` and `storage_type`.

        :param str prefix: A prefix match of keys returned.
        :param str storage_type: A type of data storage. Possible values: "data", "tmp", "result".  Default: "data".

        Returns:
            Yields keys in an iterable.

        """

        r = self.session.get(
            '/{storage_type}/list'.format(storage_type=storage_type),
            params={'prefix': prefix}
        )
        r.raise_for_status()
        for item in r.json():
            yield item
        while r.headers.get('X-NEXT', None) is not None:
            r = self.session.get(
                '/{storage_type}/list'.format(storage_type=storage_type),
                headers={'X-NEXT': r.headers['X-NEXT']},
                params={'prefix': prefix}
            )
            r.raise_for_status()
            for item in r.json():
                yield item

    def copy_from_bucket(self, src_bucket_name, src, dest, user_ns=None, storage_type='data'):
        """Copy a file from a google cloud storage bucket to your descartes
        data bucket. This requires that the dlstorage service account have
        access to your 3rd party bucket.

        :param str src_bucket_name: The name of the bucket from which you want to copy data.
        :param str src: The path to the file you want to copy inside the src_bucket_name` bucket.
        :param str dest: The new path of the file inside the descarteslabs data bucket.

        """
        if user_ns is None:
            user_ns = self.auth.namespace
        r = self.session.post(
            '/copy/{user_ns}/src/{src_bucket}/{src}/dest/{storage_type}/{dest}'.format(
                user_ns=user_ns,
                src_bucket=src_bucket_name,
                src=src,
                storage_type=storage_type,
                dest=dest
            ),
            json={}
        )
        return r.json()


storage = Storage()
storage_client = storage
