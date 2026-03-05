from collections import defaultdict
from functools import cached_property
from typing import Dict, Tuple, List, Optional

from .types import Result
from .params import Relationship
from .mappers import Mapper


class Context:
    mappers: Dict[str, Mapper]
    rels: Dict[str, Dict[str, Relationship]]
    result_mappers: List[Mapper]
    result_is_unary: Optional[bool]
    columns: Optional[Tuple[str, ...]]
    fields_to_columns: Dict[Mapper, Dict[str, Tuple[int, str]]]

    def __init__(
        self,
        result: Result,
        mappers: Dict[str, Mapper],
        columns: Tuple[str, ...],
    ):
        self.result = result
        self.mappers = {}
        self.rels = defaultdict(dict)
        self.result_mappers = [
            mappers[
                result.lower()
                if isinstance(result, str)
                else result.__name__.lower()
            ]
        ]
        self.result_is_unary = True
        self.columns = None
        self.fields_to_columns = defaultdict(dict)
        self.parse_columns(columns, mappers)

    def column_for_field(self, mapper: Mapper, field: str) -> str:
        try:
            return self.fields_to_columns[mapper][field][1]
        except KeyError as e:
            raise ValueError(
                f'For class {mapper.name} not found field {field}',
            ) from e

    def parse_columns(
        self,
        columns: Tuple[str, ...],
        mappers: Dict[str, Mapper],
    ):
        self.columns = columns
        for index, column in enumerate(columns):
            try:
                prefix, field_name = column.lower().split('__')
            except ValueError as e:
                raise ValueError(
                    f'Column {column} are not contains name of cls '
                    f'and name of field, concatenated with __'
                ) from e

            mapper = mappers.get(prefix)
            if not mapper:
                for mapper_ in mappers.values():
                    if mapper_.prefix == prefix:
                        mapper = mapper_
                        break
                else:
                    raise ValueError(f'Mapper with prefix {prefix} not found')

            if mapper not in self.mappers:
                self.mappers[mapper.name] = mapper

                for mapper_ in mappers.values():
                    for rel_field, rel in mapper_.relationships.items():
                        if (
                            rel.target_name == mapper.name and
                            rel not in self.rels[mapper_.name]
                        ):
                            self.rels[mapper_.name][rel_field] = rel

            self.fields_to_columns[mapper][field_name] = index, column

    @cached_property
    def mappers_list(self) -> List[Mapper]:
        return list(self.mappers.values())
