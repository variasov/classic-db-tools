from typing import Any, Type

from .mapper import MapperCache, compile_mapper
from .types import Cursor


class Result:

    def __init__(self, cursor: Cursor, mapper_cache: MapperCache):
        self.cursor = cursor
        self._mapper_cache = mapper_cache

    def many(self, batch_size: int = None):
        if batch_size:
            return self.cursor.fetchmany(batch_size)
        return self.cursor.fetchall()

    def one(self, raising: bool = False):
        value = self.cursor.fetchone()
        if value is None and raising:
            raise ValueError
        else:
            return value

    def scalar(self, raising: bool = False):
        value = self.one(raising)
        if value is None:
            return None
        return value[0]

    def returning(self, *params, returns: str | Type[Any]):
        mapper_id = (
            *params,
            (
                returns
                if isinstance(returns, str)
                else returns.__name__.lower()
            ),
            *(desc[0] for desc in self.cursor.description),
        )
        mapper = self._mapper_cache.get(mapper_id)
        if mapper is None:
            mapper = self._mapper_cache[mapper_id] = compile_mapper(
                params, returns, self.cursor,
            )

        return MappedResult(mapper, self.cursor)


class MappedResult:

    def __init__(self, mapper_func, cursor: Cursor):
        self.cursor = cursor
        self._mapper_func = mapper_func

    def one(self):
        # Так как маппер вернет dict_values,
        # нельзя сделать self.mapper_func(self.cursor)[0],
        # потому так
        for row in self.iter():
            return row

    def many(self):
        return list(self.iter())

    def iter(self):
        return self._mapper_func(self.cursor.fetchall()).values()

    def mapper_sources(self) -> str:
        """
        Возвращает строковое представление функции-маппера.

        Сделано ради удобства отладки.
        """
        return self._mapper_func.__generated__()
