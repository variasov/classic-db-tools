from typing import (
    Any, Iterable, Generator, Union, Dict,
    Callable, Optional, ContextManager,
)

from .mapping import Mapper, Result, Parameter, MapperQuery
from .types import Connection, Cursor, CursorParams
from .templates import Template


QueryParams = Union[CursorParams, Any]


class Query:

    def __init__(
        self,
        conn_scope: Callable[[], ContextManager[Connection]],
        mapper: Mapper,
        tmpl_factory: Callable[[], Template],
    ):
        self._conn_scope = conn_scope
        self._mapper = mapper
        self._tmpl_factory = tmpl_factory

    def map_to(
        self,
        result: Result,
        prefix: Optional[str] = None,
        /,
        **params: Parameter,
    ) -> MapperQuery[Result]:
        if prefix is None:
            if result is None:
                raise ValueError('Prefix or result must be specified')
            prefix_ = result.__name__.lower()
        else:
            prefix_ = prefix.lower()
        return MapperQuery[Result](
            conn_scope=self._conn_scope,
            mapper=self._mapper,
            params=params,
            tmpl_factory=self._tmpl_factory,
            result=prefix_,
        )

    def execute(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Cursor:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._tmpl_factory().execute(cursor, prepared)

        with self._conn_scope() as conn:
            return self._tmpl_factory().execute(conn.cursor(), prepared)

    def executemany(
        self,
        params: Iterable[QueryParams],
        cursor: Cursor = None,
    ) -> Cursor:
        prepared = [
            param if isinstance(param, (dict, tuple)) else param.__dict__
            for param in params
        ]

        if cursor is not None:
            return self._tmpl_factory().executemany(cursor, prepared)

        with self._conn_scope() as conn:
            return self._tmpl_factory().executemany(conn.cursor(), prepared)

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ):
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._all(cursor, prepared)

        with self._conn_scope() as conn:
            return self._all(conn.cursor(), prepared)

    def _all(self, cursor: Cursor, params: CursorParams) -> Iterable[Result]:
        cursor = self._tmpl_factory().execute(cursor, params)
        return cursor.fetchall()

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
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
    ) -> Generator[Any, None, None]:
        cursor = self._tmpl_factory().execute(cursor, params)
        while True:
            batch_ = cursor.fetchmany(batch)
            if not batch_:
                return
            for row in batch_:
                yield row

    def one(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._one(cursor, prepared)

        with self._conn_scope() as conn:
            return self._one(conn.cursor(), prepared)

    def _one(self, cursor: Cursor, params: CursorParams) -> Any:
        cursor = self._tmpl_factory().execute(cursor, params)
        return cursor.fetchone()

    def scalar(
        self,
        params: QueryParams = None,
        /,
        raising: bool = False,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        params = self._prepare(params, kwargs)

        if cursor is not None:
            return self._scalar(cursor, params, raising)

        with self._conn_scope() as conn:
            return self._scalar(conn.cursor(), params, raising)

    def _scalar(
        self,
        cursor: Cursor,
        params: CursorParams,
        raising: bool,
    ) -> Any:
        cursor_ = self._tmpl_factory().execute(cursor, params)
        row = cursor_.fetchone()
        if row is None and not raising:
            return None
        return row[0]

    def scalars(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._scalars(cursor, prepared, batch)

        with self._conn_scope() as conn:
            return self._scalars(conn.cursor(), prepared, batch)

    def _scalars(
        self,
        cursor: Cursor,
        params: CursorParams,
        batch: int,
    ) -> Generator[Any, None, None]:
        cursor = self._tmpl_factory().execute(cursor, params)
        while True:
            batch_ = cursor.fetchmany(batch)
            if not batch_:
                return
            for row in batch_:
                yield row[0]

    def rowcount(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> int:
        """Количество строк, обработанных запросом"""

        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._tmpl_factory().execute(cursor, prepared).rowcount

        with self._conn_scope() as conn:
            return self._tmpl_factory().execute(
                conn.cursor(), prepared,
            ).rowcount

    @staticmethod
    def _prepare(params: QueryParams, kwargs: Dict[str, Any]) -> CursorParams:
        if params is None:
            params = kwargs
        elif not isinstance(params, (dict, tuple)):
            params = params.__dict__
        return params
