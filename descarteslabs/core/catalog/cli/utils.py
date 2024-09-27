# Copyright 2018-2024 Descartes Labs.


def serialize(obj):
    d = {"id": obj.id}
    d.update(obj.serialize())
    return d
