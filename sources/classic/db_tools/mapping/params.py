from dataclasses import dataclass
from typing import Type, Any


@dataclass(slots=True, init=False, unsafe_hash=True)
class ID:
    fields: tuple[str, ...]

    def __init__(self, *fields: str):
        self.fields = tuple(field.lower() for field in fields)


@dataclass(slots=True, init=False, unsafe_hash=True)
class Name:
    content: str

    def __init__(self, content: str, /):
        self.content = content.lower()


@dataclass(frozen=True, slots=True)
class Relationship:
    left: Type[Any] | str
    field: str
    right: Type[Any] | str


@dataclass(slots=True, frozen=True)
class OneToOne(Relationship):
    pass


@dataclass(slots=True, frozen=True)
class OneToMany(Relationship):
    pass
