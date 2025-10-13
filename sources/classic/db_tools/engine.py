from types import TracebackType
from typing import (
    Callable, Any, Iterable, Generator,
    TypeAlias, Sequence, Generic, Hashable,
)
import threading
from pathlib import Path

from .dynamic.query import DynamicQuery
from .pool import ConnectionPool
from .static import StaticQuery
from .types import Cursor, CursorParams
from .transaction import Transaction
from .scoped_connection import ScopedConnection

from . import dynamic, static, mapping

Query: TypeAlias = StaticQuery | DynamicQuery


class Engine:

    def __init__(
        self,
        templates_path: str | Path,
        pool: ConnectionPool,
        commit_on_exit: bool = True,
        str_templates_static_by_default: bool = False,
        identifier_quote_char: str = "'",
    ):
        self.pool = pool
        self.conn = ScopedConnection(pool, commit_on_exit)
        self.templates_path = templates_path
        self.dynamic_templates = dynamic.DynamicQueriesFactory(
            templates_path=templates_path,
            param_style=self.pool.recognize_param_style(),
            identifier_quote_char=identifier_quote_char,
        )
        self.static_templates = static.StaticQueriesFactory(templates_path)
        self.mapper_cache = {}
        self.mapper_cache_lock = threading.Lock()
        self.str_templates_static_by_default = str_templates_static_by_default

    def get_mapper_from_cache(self, key: Hashable):
        return self.mapper_cache.get(key)

    def cache_mapper(self, key: Hashable, value: mapping.Mapper):
        with self.mapper_cache_lock:
            self.mapper_cache[key] = value

    def from_file(self, filename: str) -> 'LazyQuery':
        if filename.endswith('.sql'):
            factory = self.static_templates
        elif filename.endswith('.sql.tmpl'):
            factory = self.dynamic_templates
        else:
            raise ValueError(f'Unsupported filename extension: {filename}')
        return LazyQuery(
            engine=self,
            query_factory=lambda: factory.get(filename=filename)
        )

    def from_str(self, query: str, static: bool = None) -> 'LazyQuery':
        if static is None:
            static = self.str_templates_static_by_default

        if static is True:
            factory = self.static_templates
        elif static is False:
            factory = self.dynamic_templates
        else:
            raise ValueError(f'Unknown "static" arg value: {static}')

        return LazyQuery(
            engine=self,
            query_factory=lambda: factory.get(content=query),
        )

    @property
    def cursor(self):
        try:
            return self.conn.cursor()
        except AttributeError:
            raise AttributeError('''
                Trying to access cursor, while not in started state.
                Maybe, you forgot to enter in engine ctx?:
                >>> with engine: query.execute(...)
            ''')

    def transaction(self):
        return Transaction(self.conn.__wrapped__)

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(
            self,
            type_: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        return self.conn.__exit__(type_, value, traceback)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


class LazyQuery:

    def __init__(
        self,
        engine: Engine,
        query_factory: Callable[[], Query],
    ):
        self.engine = engine

        self.query_factory = query_factory
        self._query = None

    @property
    def query(self):
        query = self._query
        if query is None:
            query = self._query = self.query_factory()
        return query

    def return_as(
        self,
        result: mapping.Result,
        *relationships: mapping.Relationship,
    ) -> 'LazyMapper[mapping.Result]':
        return LazyMapper[mapping.Result](
            engine=self.engine,
            query_factory=self.query_factory,
            result=result,
            relationships=relationships,
        )

    def execute(
        self,
        params: CursorParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Cursor:
        return self.query.execute(
            params or kwargs,
            cursor or self.engine.cursor,
        )

    def executemany(
        self,
        params: Sequence[CursorParams],
        cursor: Cursor = None,
    ) -> Cursor:
        return self.query.executemany(params, cursor or self.engine.cursor)

    def all(
        self,
        params: CursorParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ):
        cursor = self.query.execute(
            params or kwargs,
            cursor or self.engine.cursor,
        )
        return cursor.fetchall()

    def iter(
        self,
        params: CursorParams = None,
        /,
        _batch: int = 500,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        _cursor = self.query.execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        while True:
            batch = _cursor.fetchmany(_batch)
            if not batch:
                return
            for row in batch:
                yield row

    def one(
        self,
        params: CursorParams = None,
        /,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        _cursor = self.query.execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        return _cursor.fetchone()

    def scalar(
        self,
        params: CursorParams = None,
        /,
        _raising: bool = False,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        value = self.one(
            params or kwargs,
            _raising=_raising,
            _cursor=_cursor or self.engine.cursor,
        )
        if not _raising and value is None:
            return None
        return value[0]

    def rowcount(
        self,
        params: CursorParams = None,
        /,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> int:
        """Количество строк, обработанных запросом"""
        cursor = self.query.execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        return cursor.rowcount


class LazyMapper(Generic[mapping.Result]):

    def __init__(
        self,
        engine: Engine,
        query_factory: Callable[[], Query],
        result: TypeAlias,
        relationships: Iterable[mapping.Relationship],
    ) -> None:
        self.engine = engine
        self.query_factory = query_factory
        self.result = result
        self.relationships = relationships
        self._query = None
        self._mapper = None
        self._compile_mapper = mapping.compile_mapper

    @property
    def query(self) -> Query:
        query = self._query
        if query is None:
            query = self._query = self.query_factory()
        return query

    def mapper(self, cursor: Cursor) -> Generator[Any, Any, None]:
        columns = tuple(column[0] for column in cursor.description)
        key = (self.result, *self.relationships, *columns)
        mapper = self.engine.get_mapper_from_cache(key)
        if not mapper:
            mapper = self._compile_mapper(
                self.result, self.relationships, columns,
            )
            self.engine.cache_mapper(key, mapper)
        return mapper()

    def all(
        self,
        params: CursorParams = None,
        /,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> list[mapping.Result]:
        return list(self.iter(params or kwargs, _cursor=_cursor))

    def iter(
        self,
        params: CursorParams = None,
        /,
        _batch: int | None = 500,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[mapping.Result, None, None]:
        _cursor = self.query.execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        mapper = self.mapper(_cursor)
        next(mapper)
        while True:
            if _batch:
                rows = _cursor.fetchmany(_batch)
            else:
                rows = _cursor.fetchall()
            if not rows:
                try:
                    last_obj = mapper.send(None)
                except StopIteration:
                    last_obj = None
                finally:
                    mapper.close()
                    _cursor.close()
                if last_obj:
                    yield last_obj
                break
            for row in rows:
                result = mapper.send(row)
                if result is not None:
                    yield result
                    next(mapper)

    def one(
        self,
        params: CursorParams = None,
        /,
        _batch: int = 500,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> mapping.Result:
        iterator = self.iter(
            params or kwargs,
            _batch,
            _cursor or self.engine.cursor,
        )
        try:
            result = next(iterator)
        except StopIteration:
            iterator.close()
            result = None
        return result
