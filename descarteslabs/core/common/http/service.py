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


class DefaultClientMixin:
    """
    Provides common service functionality to HTTP and Grpc clients.
    """

    @classmethod
    def get_default_client(cls):
        """Retrieve the default client.

        This client is used whenever you don't explicitly set the client.
        """

        instance = getattr(cls, "_instance", None)

        if not isinstance(instance, cls):
            instance = cls()
            cls._instance = instance

        return instance

    @classmethod
    def set_default_client(cls, client):
        """Change the default client to the given client.

        This is the client that will be used whenever you don't explicitly set the
        client
        """

        if not isinstance(client, cls):
            raise ValueError(f"client must be an instance of {cls.__name__}")

        cls._instance = client

    @classmethod
    def clear_all_default_clients(cls):
        """Clear all default clients of this class and all its subclasses."""

        cls._instance = None

        for subclass in cls.__subclasses__():
            subclass.clear_all_default_clients()
