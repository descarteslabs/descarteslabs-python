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
