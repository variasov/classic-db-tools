from collections import defaultdict
from functools import cached_property
from typing import Iterable
import inspect
import typing
from dataclasses import dataclass
from typing import Type, Any, Literal

from .params import Relationship, ID, Name
from .types import Result


@dataclass(slots=True, frozen=True)
class Mapper:
    cls: Type[Any]
    id: ID
    name: str

    @classmethod
    def create(
        cls,
        type_: Type[Any],
        id: ID = None,
        name: Name = None,
    ) -> 'Mapper':
        assert inspect.isclass(type_)
        return cls(
            type_,
            id or ID('id'),
            name.content if name else type_.__name__.lower(),
        )

    @classmethod
    def parse_annotated(cls, args: tuple[Any, ...]):
        type_param = None
        id_param = None
        name_param = None
        for arg in args:
            if isinstance(arg, ID):
                id_param = arg
            elif isinstance(arg, Name):
                name_param = arg
            elif inspect.isclass(arg):
                type_param = arg
            else:
                continue
        return cls.create(type_param, id_param, name_param)

    @classmethod
    def parse_from_annotation(cls, annotation: Type[Any]):
        origin = typing.get_origin(annotation)
        if origin is None:
            return cls.create(annotation)
        args = typing.get_args(annotation)
        if origin is typing.Annotated:
            return cls.parse_annotated(args)
        return origin, args

    @property
    def accessor_type(self) -> Literal['attr', 'item']:
        if issubclass(self.cls, dict):
            return 'item'
        else:
            return 'attr'

    @property
    def id_name(self) -> str:
        return self.name + '_id'

    @property
    def identity_map_name(self) -> str:
        return self.name + '_map'

    @property
    def last_obj_name(self) -> str:
        return f'last_{self.name}'


class Context:
    mappers: dict[str, Mapper]
    rels: dict[str, list[Relationship]]
    result_mappers: list[Mapper]
    result_is_unary: bool | None
    columns: tuple[str, ...] | None
    fields_to_columns: dict[Mapper, dict[str, str]]

    def __init__(
        self,
        result: Result,
        relationships: Iterable[Relationship],
        columns: tuple[str, ...],
    ):
        self.mappers = {}
        self.rels = defaultdict(list)
        self.result_mappers = []
        self.result_is_unary = None
        self.columns = None
        self.fields_to_columns = defaultdict(dict)

        self.parse_result(result)
        self.parse_relationships(relationships)
        self.parse_columns(columns)
        self.lineno = self._create_line_counter()

    def column_for_field(self, mapper: Mapper, field: str) -> str:
        try:
            return self.fields_to_columns[mapper][field]
        except KeyError as e:
            raise ValueError(
                f'For class {mapper.name} not found field {field}',
            ) from e

    @staticmethod
    def _create_line_counter():
        _lineno = 0

        def inc_and_return():
            nonlocal _lineno
            _lineno += 1
            return _lineno

        return inc_and_return

    def parse_mapper(self, annotation: typing.Any):
        mapper = Mapper.parse_from_annotation(annotation)
        if isinstance(mapper, Mapper):
            if mapper.name not in self.mappers:
                self.mappers[mapper.name] = mapper
        return mapper

    def mapper(self, annotation: typing.Any):
        if isinstance(annotation, str):
            return self.mappers[annotation]
        return self.parse_mapper(annotation)

    def parse_result(self, annotation: Result) -> None:
        mapper = self.parse_mapper(annotation)
        if isinstance(mapper, Mapper):
            self.result_mappers.append(mapper)
            self.result_is_unary = True
        elif isinstance(mapper, tuple):
            origin, args = mapper
            if issubclass(origin, typing.Tuple):
                self.result_is_unary = False
                for arg in args:
                    self.parse_mapper(arg)
                    if isinstance(mapper, Mapper):
                        self.result_mappers.append(mapper)

    def parse_relationships(
        self, relationships: Iterable[Relationship],
    ) -> None:
        for rel in relationships:
            if isinstance(rel.right, str):
                self.rels[rel.right].append(rel)
            else:
                mapper = self.parse_mapper(rel.right)
                self.rels[mapper.name].append(rel)

            if not isinstance(rel.left, str):
                self.parse_mapper(rel.left)

    def parse_columns(self, columns: tuple[str, ...]):
        self.columns = columns
        for column in columns:
            try:
                mapper_name, field_name = column.lower().split('__')
            except ValueError as e:
                raise ValueError(
                    f'Column {column} are not contains name of cls '
                    f'and name of field, concatenated with __'
                ) from e
            mapper = self.mappers[mapper_name]
            self.fields_to_columns[mapper][field_name] = column

    @cached_property
    def mappers_list(self) -> list[Mapper]:
        return list(self.mappers.values())
