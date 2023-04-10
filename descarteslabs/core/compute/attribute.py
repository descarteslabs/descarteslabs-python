from datetime import datetime
from typing import Type, TypeVar

T = TypeVar("T")


class Attribute(object):
    def __init__(
        self,
        type: Type[T],
        default: T = None,
        mutable: bool = True,
    ):
        self.type = type
        self.default = default
        self.mutable = mutable

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance: "ComputeObject", owner) -> T:
        default = self.default

        if callable(default):
            default = default()

        return instance._attributes.get(self.name, default)

    def __set__(self, instance: "ComputeObject", value):
        self._set(instance, value, force=False)

    def _set(self, instance: "ComputeObject", value, force=False):
        current_value = getattr(instance, self.name)

        if not force and not self.mutable and instance and current_value:
            raise ValueError(f"Unable to set mutable attribute {self.name}")

        if isinstance(value, self.type):
            instance._attributes[self.name] = value
        else:
            raise ValueError(f"Unable to assign {type(value)} to type {self.type}")


class ComputeObject(object):
    def __init__(self, **kwargs) -> None:
        self._attributes = {}
        self.__dict__ == kwargs

    def _fill_from_remote(self, data: dict):
        attributes = {
            name: instance
            for name, instance in vars(self.__class__).items()
            if isinstance(instance, Attribute)
        }

        for key, value in data.items():
            if key not in attributes:
                continue

            attributes[key]._set(self, value, force=True)


class Job(ComputeObject):
    owner: str = Attribute(str)
    creation_date: datetime = Attribute(datetime, mutable=False)
