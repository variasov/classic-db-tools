from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any, Type, Hashable, Iterable, TypedDict

from classic.sql_tools.types import Cursor


Key = str | Type[Any]


class Mapper(ABC):
    _id: str | Iterable[str]
    _key: Hashable
    _prefix: str

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def key(self) -> Hashable:
        return self._key

    @cached_property
    def id_fields(self) -> tuple[str, ...]:
        if isinstance(self._id, str):
            return (self._id, )
        elif isinstance(self._id, Iterable):
            return tuple(self._id)
        else:
            raise NotImplementedError

    @abstractmethod
    def map(self, row, fields_to_columns):
        pass

    @abstractmethod
    def one_to_one(self, left: dict[str, Any], field: str, right: Any) -> None:
        pass

    @abstractmethod
    def one_to_many(self, left: dict[str, Any], field: str, right: Any) -> None:
        pass


class BoundMapper:

    def __init__(self, cursor: Cursor, mapper: Mapper):
        self.mapper = mapper
        self.key = self.mapper.key
        self.cursor = cursor
        self.columns = {}
        self.identity_map = {}
        self.columns_field_map = {}

    def id(self, row) -> Hashable:
        return tuple((
            row[self.columns_field_map[field]]
            for field in self.mapper.id_fields
        ))

    def set_column_map(self, field, column):
        self.columns_field_map[field] = column

    def map(self, row):
        id_values = self.id(row)
        obj = self.identity_map.get(id_values, None)
        if obj is None:
            self.identity_map[id_values] = obj = self.mapper.map(
                row, self.columns_field_map,
            )
        return obj


class ToCls(Mapper):

    def __init__(
        self,
        cls: Type[Any],
        id: str = 'id',
        prefix: str = None
    ):
        self._key = cls
        self._prefix = prefix or cls.__name__.lower()
        self._id = id
        self._cls = cls

    def map(self, row, fields_to_columns):
        kwargs = {
            field: row[column]
            for field, column in fields_to_columns.items()
        }
        return self._cls(**kwargs)

    def one_to_one(self, left: dict[str, Any], field: str, right: Any) -> None:
        setattr(left, field, right)

    def one_to_many(self, left: dict[str, Any], field: str, right: Any) -> None:
        rel_attr = getattr(left, field)
        rel_attr.append(right)


class ToDict(Mapper):

    def __init__(
        self,
        key: str | Type[TypedDict],
        id: str = 'id',
        prefix: str = None
    ):
        self._key = key if isinstance(key, str) else key.__name__
        self._prefix = (prefix or key).lower()
        self._id = id

    def map(self, row, fields_to_columns):
        return {
            field: row[column]
            for field, column in fields_to_columns.items()
        }

    def one_to_one(self, left: dict[str, Any], field: str, right: Any) -> None:
        left[field] = right

    def one_to_many(self, left: dict[str, Any], field: str, right: Any) -> None:
        rel_attr = left.get(field)
        if rel_attr is None:
            left[field] = rel_attr = []

        rel_attr.append(right)


class ToNamedTuple(Mapper):
    pass


class Relationship:
    _left: Key
    _right: Key

    def __init__(self, left: Key, field: str, right: Key) -> None:
        self._left = left
        self._right = right
        self._field = field

    def map(self, objects: dict[Key, Any]) -> Any:
        pass


class OneToOne(Relationship):

    def map(self, objects: dict[Key, tuple[Any, BoundMapper]]):
        left_obj, left_mapper = objects[self._left]
        right_obj, right_mapper = objects[self._right]

        left_mapper.mapper.one_to_one(left_obj, self._field, right_obj)


class OneToMany(Relationship):

    def map(self, objects: dict[Key, tuple[Any, BoundMapper]]):
        left_obj, left_mapper = objects[self._left]
        right_obj, right_mapper = objects[self._right]

        left_mapper.mapper.one_to_many(left_obj, self._field, right_obj)


def automap(cls):
    pass
