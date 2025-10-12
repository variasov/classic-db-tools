from dataclasses import dataclass
from typing import Type, Any, Sequence, Literal


@dataclass(slots=True, init=False, unsafe_hash=True)
class ID:
    fields: tuple[str, ...]

    def __init__(self, *fields: str):
        self.fields = tuple(fields)


@dataclass(frozen=True, slots=True)
class ClsMapper:
    cls: Type[Any]
    id_fields: str | Sequence[str] = 'id'

    @property
    def name(self) -> str:
        return self.cls.__name__.lower()

    @property
    def accessor_type(self) -> Literal['attr', 'item']:
        if issubclass(self.cls, dict):
            return 'item'
        else:
            return 'attr'

    @property
    def id_is_unary(self) -> bool:
        return isinstance(self.id_fields, str)

    @property
    def id_name(self) -> str:
        return self.name + '_id'

    @property
    def identity_map_name(self) -> str:
        return self.name + '_map'

    @property
    def last_obj_name(self) -> str:
        return f'last_{self.name}'


@dataclass(frozen=True, slots=True)
class Relationship:
    left: Type[Any]
    field: str
    right: Type[Any]

    @property
    def name_of_left(self) -> str:
        return self.left.__name__.lower()

    @property
    def name_of_right(self) -> str:
        return self.right.__name__.lower()


@dataclass(slots=True, frozen=True)
class OneToOne(Relationship):
    pass


@dataclass(slots=True, frozen=True)
class OneToMany(Relationship):
    pass
