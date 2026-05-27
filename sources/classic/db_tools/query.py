from typing import (
    Any, Iterable, Generator, List, Sequence, Union, Dict,
    Callable, Optional, cast,
)

from .mapping import Mapper, Result, Parameter, MapperQuery
from .types import Row, Cursor
from .templates import Template
from .conn_scope import ConnectionScope


QueryParams = Union[Dict[str, Any], Any]


class Query:

    def __init__(
        self,
        conn_scope: Callable[[], ConnectionScope],
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
        cursor: Cursor | None = None,
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
        cursor: Cursor | None = None,
    ) -> Cursor:
        prepared = cast(
            List[Dict[str, Any]],
            [
                param if isinstance(param, (dict, tuple)) else param.__dict__
                for param in params
            ]
        )

        if cursor is not None:
            return self._tmpl_factory().executemany(cursor, prepared)

        with self._conn_scope() as conn:
            return self._tmpl_factory().executemany(conn.cursor(), prepared)

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor | None = None,
        **kwargs: Any,
    ) -> Sequence[Row]:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._all(cursor, prepared)

        with self._conn_scope() as conn:
            return self._all(conn.cursor(), prepared)

    def _all(self, cursor: Cursor, params: Dict[str, Any]) -> Sequence[Row]:
        cursor = self._tmpl_factory().execute(cursor, params)
        return cursor.fetchall()

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor | None = None,
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
        params: Dict[str, Any],
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
        cursor: Cursor | None = None,
        **kwargs: Any,
    ) -> Any:
        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._one(cursor, prepared)

        with self._conn_scope() as conn:
            return self._one(conn.cursor(), prepared)

    def _one(self, cursor: Cursor, params: Dict[str, Any]) -> Any:
        cursor = self._tmpl_factory().execute(cursor, params)
        return cursor.fetchone()

    def scalar(
        self,
        params: QueryParams = None,
        /,
        raising: bool = False,
        cursor: Cursor | None = None,
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
        params: Dict[str, Any],
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
        cursor: Cursor | None = None,
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
        params: Dict[str, Any],
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
        cursor: Cursor | None = None,
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
    def _prepare(params: QueryParams, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        params_: Dict[str, Any] = kwargs
        if params is None:
            params_ = kwargs
        elif not isinstance(params, (dict, tuple)):
            params_ = params.__dict__
        return params_
