import inspect
from collections import defaultdict
import typing
from functools import cached_property
from typing import Any, Type, Iterable

from .params import ClsMapper, Relationship, ID
from .types import Result


class Context:

    def __init__(
        self,
        result: Result,
        relationships: Iterable[Relationship],
        columns: tuple[str, ...],
    ):
        self.registry = defaultdict(list)
        self.result_mappers = []
        self.result_is_unary = None
        self.columns = None
        self.fields_to_columns = defaultdict(dict)

        self.parse_result(result)
        self.parse_relationships(relationships)
        self.parse_columns(columns)
        self.lineno = self._create_line_counter()

    def column_for_field(self, mapper: ClsMapper, field: str) -> str:
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

    def add_cls(self, type_: Type[Any]):
        id_fields = 'id'
        cls = type_
        if typing.get_origin(type_) is typing.Annotated:
            args = typing.get_args(type_)
            cls = args[0]
            for arg in args:
                if isinstance(arg, ID):
                    id_fields = arg.fields
                    break

        assert inspect.isclass(cls)
        mapper = ClsMapper(cls, id_fields)
        self.registry[mapper] = []
        return mapper

    def parse_result(self, result: Result) -> None:
        result_origin = typing.get_origin(result)
        if result_origin is None:
            self.result_is_unary = True
            mapper = self.add_cls(result)
            self.result_mappers.append(mapper)
            return

        self.result_is_unary = False
        for type_ in typing.get_args(result):
            mapper = self.add_cls(type_)
            self.result_mappers.append(mapper)

    def parse_relationships(
        self, relationships: Iterable[Relationship],
    ) -> None:
        for relation in relationships:
            self.registry[self.add_cls(relation.right)].append(relation)
            self.registry[self.add_cls(relation.left)] = []

    def parse_columns(self, columns: tuple[str, ...]):
        self.columns = columns
        for column in columns:
            try:
                cls_name, field_name = column.split('__')
            except ValueError as e:
                raise ValueError(
                    f'Column {column} are not contains name of cls '
                    f'and name of field, concated with __'
                ) from e
            for mapper in self.mappers:
                if cls_name == mapper.name:
                    self.fields_to_columns[mapper][field_name] = column
                    break

    @cached_property
    def mappers(self) -> list[ClsMapper]:
        return list(self.registry.keys())
