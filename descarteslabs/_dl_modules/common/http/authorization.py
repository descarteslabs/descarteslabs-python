def add_bearer(token):
    """For use with Authorization headers, add "Bearer "."""
    if token:
        return ("Bearer " if isinstance(token, str) else b"Bearer ") + token
    else:
        return token


def remove_bearer(token):
    """For use with Authorization headers, strip any "Bearer "."""
    if isinstance(token, (str, bytes)) and token.lower().startswith(
        "bearer " if isinstance(token, str) else b"bearer "
    ):
        return token[7:]
    else:
        return token


def add_basic(token):
    """For use with Authorization headers, add "Basic "."""
    if token:
        return ("Basic " if isinstance(token, str) else b"Basic ") + token
    else:
        return token


def remove_basic(token):
    """For use with Authorization headers, strip any "Basic "."""
    if isinstance(token, (str, bytes)) and token.lower().startswith(
        "basic " if isinstance(token, str) else b"basic "
    ):
        return token[6:]
    else:
        return token
