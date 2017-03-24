import descarteslabs as dl


def is_external_user():
    return "descartes:team" not in dl.descartes_auth.payload["groups"]
