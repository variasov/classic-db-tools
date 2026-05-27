from functools import partial
from typing import (
    Any, Generator, List, Generic, Dict,
    Callable, Optional, ContextManager, Union,
)

from ..types import Connection, Cursor, CursorParams
from ..templates import Template

from .types import Result, MapperFunc
from .mapper import Mapper
from .params import Parameter, create_mapping


QueryParams = Union[CursorParams, Any]


class MapperQuery(Generic[Result]):

    def __init__(
        self,
        conn_scope: Callable[[], ContextManager[Connection]],
        tmpl_factory: Callable[[], Template],
        result: str,
        mapper: Mapper,
        params: Dict[str, Parameter],
    ) -> None:
        self._conn_scope = conn_scope
        self._tmpl_factory = tmpl_factory
        self._result = result.lower()
        self._mapper = mapper
        self._mapping = create_mapping(**params)

    def sources(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> str:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._sources(cursor, prepared)

        with self._conn_scope() as conn:
            return self._sources(conn.cursor(), prepared)

    def _sources(self, cursor: Cursor, params: CursorParams) -> str:
        mapper = self._execute(cursor, params)
        return getattr(mapper, 'sources', lambda: '')()

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> List[Result]:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._all(cursor, prepared)

        with self._conn_scope() as conn:
            return self._all(conn.cursor(), prepared)

    def _all(self, cursor: Cursor, params: CursorParams) -> list[Result]:
        mapper = self._execute(cursor, params)
        return list(mapper(cursor.fetchall()))

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: Optional[int] = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Result, None, None]:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._iter(cursor, prepared, batch)

        with self._conn_scope() as conn:
            return self._iter(conn.cursor(), prepared, batch)

    def _iter(
        self,
        cursor: Cursor,
        params: CursorParams,
        batch: int,
    ) -> Generator[Result, None, None]:
        mapper = self._execute(cursor, params)
        for obj in mapper(self._iterate_rows(cursor, batch)):
            yield obj

    def one(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Result:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._one(cursor, prepared, batch)

        with self._conn_scope() as conn:
            return self._one(conn.cursor(), prepared, batch)

    def _one(
        self,
        cursor: Cursor,
        params: CursorParams,
        batch: int,
    ) -> Result:
        mapper = self._execute(cursor, params)
        iterator = mapper(self._iterate_rows(cursor, batch))
        try:
            result = next(iterator)
        except StopIteration:
            result = None
        finally:
            iterator.close()
        return result

    @staticmethod
    def _prepare(params: QueryParams, kwargs: Dict[str, Any]) -> CursorParams:
        if params is None:
            params = kwargs
        elif not isinstance(params, (dict, tuple)):
            params = params.__dict__
        return params

    def _execute(self, cursor: Cursor, params: CursorParams) -> MapperFunc:
        self._tmpl_factory().execute(cursor, params)
        return self._mapper.func_for_cursor(
            cursor, self._mapping, self._result,
        )

    @staticmethod
    def _iterate_rows(cursor: Cursor, batch: int) -> Generator[
        Result, None, None]:
        if batch:
            fetch = partial(cursor.fetchmany, batch)
        else:
            fetch = cursor.fetchall
        while True:
            rows = fetch()
            if not rows:
                return
            for row in rows:
                yield row
