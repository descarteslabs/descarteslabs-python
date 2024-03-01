def serialize(obj):
    d = {"id": obj.id}
    d.update(obj.serialize())
    return d
