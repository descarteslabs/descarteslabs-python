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
import six
import warnings

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.service import Service, ThirdPartyService
from descarteslabs.client.exceptions import NotFoundError


class Storage(Service):
    """
    The Storage API provides a mechanism to store arbitrary data and later retrieve it using
    simple key-value pair semantics.
    """

    TIMEOUT = (9.5, 120)

    def __init__(self, url=None, auth=None, retries=None):
        """
        :param str url: A HTTP URL pointing to a version of the storage service
            (defaults to current version)
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param urllib3.util.retry.Retry retries: A custom retry configuration
            used for all API requests (defaults to a reasonable amount of retries)
        """
        if auth is None:
            auth = Auth()

        warnings.simplefilter("always", DeprecationWarning)
        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_STORAGE_URL",
                "https://platform.descarteslabs.com/storage/v1",
            )

        self._gcs_upload_service = ThirdPartyService()

        super(Storage, self).__init__(url, auth=auth, retries=retries)

    def get_signed_url(self, key, storage_type="data"):
        """
        Gets a temporary signed URL for downloading data stored at a key.

        :param str key: A key a storage blob
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :return: A temporary signed URL used to download the data
        :rtype: str
        """
        r = self.session.get(
            "/{storage_type}/get_signed_url/{key}".format(
                storage_type=storage_type, key=key
            )
        )
        r.raise_for_status()
        return r.content.decode("ascii")

    def get_upload_url(self, key, storage_type="data", **kwargs):
        """
        Gets a temporary signed URL for uploading data to a key.

        :param str key: A key a storage blob
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :return: A temporary signed URL used to upload data to
        :rtype: str
        """
        r = self.session.get(
            "/{storage_type}/new_resumable_url/{key}".format(
                storage_type=storage_type, key=key
            ),
            params=kwargs,
        )
        r.raise_for_status()
        return r.content.decode("ascii")

    def delete(self, key, storage_type="data"):
        """
        Deletes the data stored at location ``key`` with storage type
        ``storage_type``.

        :param str key: A key identifying an existing storage blob
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :raises descarteslabs.client.exceptions.NotFoundError: if no data exists
            for the given key and storage type
        """

        self.session.delete(
            "/{storage_type}/{key}".format(storage_type=storage_type, key=key)
        )

    def set(self, key, value, storage_type="data"):
        """
        Stores a value at location ``key`` with storage type ``storage_type``.

        :param str key: A key identifying a storage blob
        :param str|file-like value: A string value or file-like object to be stored
            at location ``key``
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        """

        rurl = self.get_upload_url(key, storage_type=storage_type)

        self._gcs_upload_service.session.put(rurl, data=value)
        return

    def get(self, key, storage_type="data"):
        """
        Retrieves data stored at location ``key`` with storage type ``storage_type``.

        :param str key: A key identifying a storage blob
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :return: the data stored at ``key`` as a string
        :rtype: str
        :raises descarteslabs.client.exceptions.NotFoundError: if no data exists
            for the given key and storage type
        """

        r = self.session.get(
            "/{storage_type}/get/{key}".format(storage_type=storage_type, key=key)
        )
        r.raise_for_status()
        return r.content

    def exists(self, key, storage_type="data"):
        """
        Determine if there is data stored at location ``key`` with storage type
        ``storage_type``.

        :param str key: A key identifying a storage blob
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.

        :return: whether there is data at location ``key``
        :rtype: bool
        """
        r = None
        try:
            r = self.session.head(
                "/{storage_type}/get/{key}".format(storage_type=storage_type, key=key)
            )
        except NotFoundError:
            return False

        return r and r.ok

    def list(self, prefix=None, storage_type="data"):
        """
        Lists keys stored under the given ``storage_type``, optionally with a required
        ``prefix``. Includes up to 1000 results, use :meth:`Storage.iter_list` to iterate
        over more results.

        :param str prefix: Only include keys with this prefix if given
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :return: A list of matching keys
        :rtype: list(str)
        """

        r = self.session.get(
            "/{storage_type}/list".format(storage_type=storage_type),
            params={"prefix": prefix},
        )
        r.raise_for_status()
        return r.json()

    def iter_list(self, prefix=None, storage_type="data"):
        """
        Yields keys stored under the given ``storage_type``, optionally with a required
        ``prefix``.

        :param str prefix: Only include keys with this prefix if given
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :return: An iterator over all matching keys
        :rtype: generator(str)
        """

        r = self.session.get(
            "/{storage_type}/list".format(storage_type=storage_type),
            params={"prefix": prefix},
        )
        r.raise_for_status()
        for item in r.json():
            yield item
        while r.headers.get("X-NEXT", None) is not None:
            r = self.session.get(
                "/{storage_type}/list".format(storage_type=storage_type),
                headers={"X-NEXT": r.headers["X-NEXT"]},
                params={"prefix": prefix},
            )
            r.raise_for_status()
            for item in r.json():
                yield item

    def set_file(self, key, file_obj, storage_type="data"):
        """
        Stores data from a file or file-like object at location ``key`` with storage
        type ``storage_type``.

        :param str key: A key identifying a storage blob
        :param str|file-like value: A file name or a file-like object with data to be
            stored at location ``key``
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        """
        rurl = self.get_upload_url(key, storage_type=storage_type)

        if isinstance(file_obj, six.string_types):
            with open(file_obj, "rb") as f:
                self._gcs_upload_service.session.put(rurl, data=f)
        else:
            self._gcs_upload_service.session.put(rurl, data=file_obj)

        return

    def get_file(self, key, file_obj, storage_type="data"):
        """
        Retrieves data stored at location ``key`` with storage type ``storage_type``
        and write it to a file.

        :param str key: A key identifying a storage blob
        :param str|file-like file_obj: File-like object or name of file to which retrieved
            data will be written. If this is a file-like object it must accept bytes (for
            example, a file opened in binary mode such as with ``open(filename, 'wb')``).
        :param str storage_type: A type of data storage. Possible values:
            ``"data"``, ``"tmp"``, ``"result"``. Default: ``"data"``.
        :raises descarteslabs.client.exceptions.NotFoundError: if no data exists
            for the given key and storage type
        """
        r = self.session.get(
            "/{storage_type}/get/{key}".format(storage_type=storage_type, key=key),
            stream=True,
        )
        r.raise_for_status()

        if isinstance(file_obj, six.string_types):
            with open(file_obj, "wb") as f:
                for chunk in r.iter_content(chunk_size=None):
                    if chunk:
                        f.write(chunk)
        else:
            for chunk in r.iter_content(chunk_size=None):
                if chunk:
                    file_obj.write(chunk)


storage = Storage()
storage_client = storage
