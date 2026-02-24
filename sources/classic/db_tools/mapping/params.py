from dataclasses import dataclass
from typing import Type, Any, Tuple, Union


@dataclass(init=False, unsafe_hash=True)
class ID:
    fields: Tuple[str, ...]

    def __init__(self, *fields: str):
        self.fields = tuple(field.lower() for field in fields)


@dataclass(init=False, unsafe_hash=True)
class Name:
    content: str

    def __init__(self, content: str, /):
        self.content = content.lower()


@dataclass(frozen=True)
class ReduceNone:
    value: bool


@dataclass(frozen=True)
class Relationship:
    left: Union[Type[Any], str]
    field: str
    right: Union[Type[Any], str]


@dataclass(frozen=True)
class OneToOne(Relationship):
    pass


@dataclass(frozen=True)
class OneToMany(Relationship):
    pass
