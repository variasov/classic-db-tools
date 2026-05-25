from functools import wraps, partial
import logging
from os import PathLike
from types import ModuleType
from typing import (
    Any, Iterable, Generator, Union, List,
    Sequence, Generic, Hashable,
    Callable, Optional, Type,
)
import threading
from pathlib import Path

from frozendict import frozendict
from .mapping import (
    Result, Parameter,
    MapperFunc, compile_mapper_func,
    Mapping, create_mapping,
)
from .pool import ConnectionPool
from .types import Connection, Cursor, CursorParams, Row
from .transaction import Transaction
from .conn_scope import ConnectionScope
from .scope import Scope

from . import dynamic, static


QueryParams = Union[CursorParams, Any]


class Engine:

    def __init__(
        self,
        driver: ModuleType,
        factory: Callable[[], Connection] = None,
        /,
        pool_class: Type[ConnectionPool] = ConnectionPool,
        pool_kwargs: Mapping[str, Any] = None,
        templates_dirs: Union[
            str, PathLike, Sequence[Union[str, PathLike]]
        ] = None,
        default_mapping: Union[Mapping, dict[str, Parameter]] = None,
        logger: logging.Logger = None,
        str_templates_static_by_default: bool = False,
        identifier_quote_char: Optional[str] = None,
    ):
        self.driver = driver
        self.factory = factory or driver.connect

        self.pool_cls = pool_class
        self.pool = pool_class(driver, self.factory, **pool_kwargs)
        self.current = Scope()
        self.tx_cls = Transaction.implementations[self.driver]

        if default_mapping:
            self.mapping = create_mapping(**default_mapping)
        else:
            self.mapping = frozendict()

        self.logger = logger or logging.getLogger('classic-db-tools')

        if templates_dirs is None:
            self.templates_paths = []
        elif isinstance(templates_dirs, str):
            self.templates_paths = [templates_dirs]
        elif isinstance(templates_dirs, Path):
            self.templates_paths = [str(templates_dirs)]
        elif isinstance(templates_dirs, Sequence):
            self.templates_paths = templates_dirs
        else:
            raise ValueError(
                'templates_paths not an str, '
                'PathLike or Sequence[Str | PathLike], but %s',
                templates_dirs,
            )

        self.dynamic_templates = dynamic.DynamicQueriesCache(
            self.logger,
            templates_paths=self.templates_paths,
            identifier_quote_char=identifier_quote_char,
        )
        self.static_templates = static.StaticQueriesCache(
            self.logger,
            templates_paths=self.templates_paths,
        )
        self.mapper_cache = {}
        self.mapper_cache_lock = threading.Lock()
        self.str_templates_static_by_default = str_templates_static_by_default

    def get_mapper_from_cache(self, key: Hashable):
        return self.mapper_cache.get(key)

    def cache_mapper(self, key: Hashable, value: MapperFunc):
        with self.mapper_cache_lock:
            self.mapper_cache[key] = value

    def query_from(self, filename: str) -> 'Query':
        if filename.endswith('.sql'):
            create_lazy = self.static_templates.create_lazy
        elif filename.endswith('.sql.tmpl'):
            create_lazy = self.dynamic_templates.create_lazy
        else:
            raise ValueError(f'Unsupported filename extension: {filename}')
        return Query(self, create_lazy(filename=filename))

    def query(self, content: str, static: bool = None) -> 'Query':
        if static is None:
            static = self.str_templates_static_by_default

        if static is True:
            create_lazy = self.static_templates.create_lazy
        elif static is False:
            create_lazy = self.dynamic_templates.create_lazy
        else:
            raise ValueError(f'Unknown "static" arg value: {static}')

        return Query(self, create_lazy(content=content))

    def transaction(
        self,
        fn: Callable[[Any], Any] = None,
        /,
        commit: bool = True,
        **tx_kwargs: Any,
    ):
        if fn:
            @wraps(fn)
            def wrapper(*args, **kwargs):
                with self.transaction(commit=commit, **tx_kwargs):
                    return fn(*args, **kwargs)
            return wrapper
        else:
            return self.tx_cls(self.pool, self.current, commit)

    def conn(
        self,
        fn: Callable[[Any], Any] = None,
        /,
    ):
        if fn:
            @wraps(fn)
            def wrapper(*args, **kwargs):
                with self.conn():
                    return fn(*args, **kwargs)
            return wrapper

        return ConnectionScope(self.pool, self.current)

    def execute_lazy_query(
        self,
        query,
        params: QueryParams = None,
        cursor: Cursor = None,
    ) -> Cursor:
        if cursor is not None:
            return query().execute(params, cursor)

        with self.conn() as conn:
            return query().execute(params, conn.cursor())

    def execute_many_lazy_query(
        self,
        query,
        params: QueryParams = None,
        cursor: Cursor = None,
    ) -> Cursor:
        if cursor is not None:
            return query().executemany(params, cursor)

        with self.conn() as conn:
            return query().executemany(params, conn.cursor())


def _prepare(params, kwargs):
    if params is None:
        params = kwargs
    elif not isinstance(params, (dict, tuple)):
        params = params.__dict__
    return params


def _iterate_rows(cursor, batch):
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


class Query:

    def __init__(
        self,
        engine: Engine,
        lazy_query,
    ):
        self.engine = engine
        self._lazy_query = lazy_query

    def map_to(
        self,
        result: Result,
        prefix: Optional[str] = None,
        /,
        **params: Parameter,
    ) -> 'MappedQuery[Result]':
        if prefix is None:
            if result is None:
                raise ValueError('Prefix or result must be specified')
            prefix_ = result.__name__.lower()
        else:
            prefix_ = prefix.lower()
        mapping_ = create_mapping(**params) if params else self.engine.mapping
        return MappedQuery[Result](
            engine=self.engine,
            lazy_query=self._lazy_query,
            result=prefix_,
            mapping=mapping_,
        )

    def execute(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Cursor:
        prepared = _prepare(params, kwargs)

        if cursor is not None:
            return self._lazy_query().execute(prepared, cursor)

        with self.engine.conn() as conn:
            return self._lazy_query().execute(prepared, conn.cursor())

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
            return self._lazy_query().executemany(prepared, cursor)

        with self.engine.conn() as conn:
            return self._lazy_query().executemany(prepared, conn.cursor())

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ):
        prepared = _prepare(params, kwargs)
        
        if cursor is not None:
            return self._all(cursor, prepared)

        with self.engine.conn() as conn:
            return self._all(conn.cursor(), prepared)

    def _all(self, cursor, params):
        cursor = self._lazy_query().execute(params, cursor)
        return cursor.fetchall()

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        prepared = _prepare(params, kwargs)
        
        if cursor is not None:
            return self._iter(cursor, prepared, batch)

        with self.engine.conn() as conn:
            return self._iter(conn.cursor(), prepared, batch)
    
    def _iter(self, cursor, params, batch):
        cursor = self._lazy_query().execute(params, cursor)
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
        prepared = _prepare(params, kwargs)
        
        if cursor is not None:
            return self._one(cursor, prepared)

        with self.engine.conn() as conn:
            return self._one(conn.cursor(), prepared)
    
    def _one(self, cursor, params):
        cursor = self._lazy_query().execute(params, cursor)
        return cursor.fetchone()

    def scalar(
        self,
        params: QueryParams = None,
        /,
        raising: bool = False,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        params = _prepare(params, kwargs)

        if cursor is not None:
            return self._scalar(cursor, params, raising)

        with self.engine.conn() as conn:
            return self._scalar(conn.cursor(), params, raising)
    
    def _scalar(self, cursor, params, raising):
        cursor_ = self._lazy_query().execute(params, cursor)
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
        prepared = _prepare(params, kwargs)
        
        if cursor is not None:
            return self._scalars(cursor, prepared, batch)

        with self.engine.conn() as conn:
            return self._scalars(conn.cursor(), prepared, batch)
    
    def _scalars(self, cursor, params, batch):
        cursor = self._lazy_query().execute(params, cursor)
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
        
        prepared = _prepare(params, kwargs)

        if cursor is not None:
            return self._lazy_query().execute(prepared, cursor).rowcount

        with self.engine.conn() as conn:
            cursor_ = self._lazy_query().execute(prepared, conn.cursor())
            return cursor_.rowcount


class MappedQuery(Generic[Result]):

    def __init__(
        self,
        engine: Engine,
        lazy_query,
        result: str,
        mapping: Mapping,
    ) -> None:
        self.engine = engine
        self._lazy_query = lazy_query
        self.result = result.lower()
        self.mapping = mapping
        self._mapper = None

        # Alias for test simplicity
        self._compile_mapper = compile_mapper_func

    def mapper_func(self, cursor: Cursor) -> Callable[
        [Iterable[Row]],
        Generator[Any, Any, None]
    ]:
        columns = tuple(column[0] for column in cursor.description)
        key = (self.result, self.mapping, columns)
        mapper = self.engine.get_mapper_from_cache(key)
        if not mapper:
            mapper = self._compile_mapper(
                self.result, self.mapping, columns,
            )
            self.engine.cache_mapper(key, mapper)
        return mapper

    def sources(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> str:
        prepared = _prepare(params, kwargs)
        
        if cursor is not None:
            return self._sources(cursor, prepared)

        with self.engine.conn() as conn:
            return self._sources(conn.cursor(), prepared)
    
    def _sources(self, cursor, params):
        cursor_ = self._lazy_query().execute(params, cursor)
        func = self.mapper_func(cursor_)
        return getattr(func, 'sources', lambda: '')()

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> List[Result]:
        prepared = _prepare(params, kwargs)

        if cursor is not None:
            return self._all(cursor, prepared)

        with self.engine.conn() as conn:
            return self._all(conn.cursor(), prepared)

    def _all(self, cursor, params):
        cursor_ = self._lazy_query().execute(params, cursor)
        mapper = self.mapper_func(cursor_)
        return [
            obj for obj in mapper(cursor_.fetchall())
        ]

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: Optional[int] = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Result, None, None]:
        prepared = _prepare(params, kwargs)

        if cursor is not None:
            return self._iter(cursor, prepared, batch)

        with self.engine.conn() as conn:
            return self._iter(conn.cursor(), prepared, batch)

    def _iter(self, cursor, params, batch):
        cursor_ = self._lazy_query().execute(params, cursor)
        mapper = self.mapper_func(cursor_)
        for obj in mapper(_iterate_rows(cursor_, batch)):
            yield obj

    def one(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Result:
        prepared = _prepare(params, kwargs)

        if cursor is not None:
            return self._one(cursor, prepared, batch)

        with self.engine.conn() as conn:
            return self._one(conn.cursor(), prepared, batch)

    def _one(self, cursor, params, batch):
        cursor_ = self._lazy_query().execute(params, cursor)
        mapper = self.mapper_func(cursor_)
        iterator = mapper(_iterate_rows(cursor_, batch))
        try:
            result = next(iterator)
        except StopIteration:
            result = None
        finally:
            iterator.close()
        return result
