class ClientError(Exception):
    """ Base class """
    pass


class AuthError(ClientError):
    pass


class OauthError(AuthError):
    pass


class ServerError(Exception):
    status = 500


class BadRequestError(ServerError):
    status = 400


class NotFoundError(ServerError):
    status = 404


class RateLimitError(ServerError):
    status = 429
