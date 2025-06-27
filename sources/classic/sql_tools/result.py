from typing import Any, Type

from .mapper import Mapper, BoundMapper, Relationship
from .types import Cursor


class Result:

    def __init__(self, cursor):
        self.cursor = cursor

    def many(self, batch_size: int = None):
        if batch_size:
            return self.cursor.fetchmany(batch_size)
        return self.cursor.fetchall()

    def one(self, raising: bool = False):
        value = self.cursor.fetchone()
        if raising and value is None:
            raise ValueError
        else:
            return value

    def scalar(self, raising: bool = False):
        value = self.one(raising)
        if not raising and value is None:
            return None
        return value[0]

    def returning(self, *params, returns: str | Type[Any]):
        return MappedResult(self.cursor, *params, returns=returns)


class MappedResult:

    def __init__(self, cursor: Cursor, *params, returns: str | Type[Any]):
        self.cursor = cursor
        self.returns = returns

        self.mappers = {}
        mappers = {
            param.prefix: BoundMapper(cursor, param)
            for param in params
            if isinstance(param, Mapper)
        }
        for index, column_desc in enumerate(self.cursor.description):
            try:
                prefix, field_name = column_desc[0].split('__')
            except ValueError:
                continue

            mapper = mappers.get(prefix)
            if not mapper:
                continue

            mapper.set_column_map(field_name, index)
            self.mappers[mapper.key] = mapper

        self.relationships = tuple((
            param
            for param in params
            if isinstance(param, Relationship)
        ))

    def one(self):
        return self._parse()[0]

    def many(self):
        return self._parse()

    def _parse(self):
        for row in self.cursor.fetchall():
            objects = {}
            for mapper in self.mappers.values():
                objects[mapper.key] = mapper.map(row), mapper

            for rel in self.relationships:
                rel.map(objects)

        return list(self.mappers[self.returns].identity_map.values())
