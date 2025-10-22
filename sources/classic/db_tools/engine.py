from functools import wraps, partial
from os import PathLike
from types import TracebackType
from typing import (
    Any, Iterable, Generator,
    TypeAlias, Sequence, Generic, Hashable, Type, TypeVar, Callable,
)
import threading
from pathlib import Path

from classic.components import add_extra_annotation, doublewrap

from .pool import ConnectionPool
from .types import Cursor, CursorParams, Row
from .transaction import Transaction
from .scoped_connection import ScopedConnection

from . import dynamic, static, mapping


class Engine:

    def __init__(
        self,
        templates_paths: str | PathLike | Sequence[str | PathLike],
        pool: ConnectionPool,
        commit_on_exit: bool = True,
        str_templates_static_by_default: bool = False,
        identifier_quote_char: str = "'",
    ):
        self.pool = pool
        self.conn = ScopedConnection(pool, commit_on_exit)
        if isinstance(templates_paths, str):
            self.templates_paths = [templates_paths]
        elif isinstance(templates_paths, Path):
            self.templates_paths = [str(templates_paths)]
        elif isinstance(templates_paths, Sequence):
            self.templates_paths = templates_paths
        else:
            raise ValueError(
                'templates_paths not an str, '
                'PathLike or Sequence[Str | PathLike]'
            )
        self.dynamic_templates = dynamic.DynamicQueriesCache(
            templates_paths=self.templates_paths,
            identifier_quote_char=identifier_quote_char,
        )
        self.static_templates = static.StaticQueriesCache(
            templates_paths=self.templates_paths,
        )
        self.mapper_cache = {}
        self.mapper_cache_lock = threading.Lock()
        self.str_templates_static_by_default = str_templates_static_by_default

    def get_mapper_from_cache(self, key: Hashable):
        return self.mapper_cache.get(key)

    def cache_mapper(self, key: Hashable, value: mapping.Mapper):
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

    @property
    def cursor(self):
        try:
            return self.conn.cursor()
        except AttributeError:
            raise AttributeError('''
                Trying to access cursor, while not in started state.
                Maybe, you forgot to enter in engine ctx?:
                >>> with engine:
                ...     query.execute(...)
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


class Query:

    def __init__(
        self,
        engine: Engine,
        lazy_query,
    ):
        self.engine = engine
        self._lazy_query = lazy_query

    def return_as(
        self,
        result: mapping.Result,
        *relationships: mapping.Relationship,
    ) -> 'MappedQuery[mapping.Result]':
        return MappedQuery[mapping.Result](
            engine=self.engine,
            lazy_query=self._lazy_query,
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
        return self._lazy_query().execute(
            params or kwargs,
            cursor or self.engine.cursor,
        )

    def executemany(
        self,
        params: Sequence[CursorParams],
        cursor: Cursor = None,
    ) -> Cursor:
        return self._lazy_query().executemany(
            params, cursor or self.engine.cursor,
        )

    def all(
        self,
        params: CursorParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ):
        cursor = self._lazy_query().execute(
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
        _cursor = self._lazy_query().execute(
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
        _cursor = self._lazy_query().execute(
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
        cursor = self._lazy_query().execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        return cursor.rowcount


class MappedQuery(Generic[mapping.Result]):

    def __init__(
        self,
        engine: Engine,
        lazy_query,
        result: TypeAlias,
        relationships: Iterable[mapping.Relationship],
    ) -> None:
        self.engine = engine
        self._lazy_query = lazy_query
        self.result = result
        self.relationships = relationships
        self._mapper = None
        self._compile_mapper = mapping.compile_mapper

    def mapper(self, cursor: Cursor) -> Callable[
        [Iterable[Row]],
        Generator[Any, Any, None]
    ]:
        columns = tuple(column[0] for column in cursor.description)
        key = (self.result, *self.relationships, *columns)
        mapper = self.engine.get_mapper_from_cache(key)
        if not mapper:
            mapper = self._compile_mapper(
                self.result, self.relationships, columns,
            )
            self.engine.cache_mapper(key, mapper)
        return mapper

    def sources(
        self,
        params: CursorParams = None,
        /,
        _cursor: Cursor = None,
        **kwargs: Any,
    ):
        cursor = self._lazy_query().execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        return self.mapper(cursor).sources()

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
        _cursor = self._lazy_query().execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        mapper = self.mapper(_cursor)

        if _batch:
            fetch = partial(_cursor.fetchmany, _batch)
        else:
            fetch = _cursor.fetchall

        def rows_iter():
            while True:
                rows = fetch()
                if not rows:
                    return
                for row in rows:
                    yield row

        for obj in mapper(rows_iter()):
            yield obj

        # next(mapper_instance)
        # while True:
        #     if _batch:
        #         rows = _cursor.fetchmany(_batch)
        #     else:
        #         rows = _cursor.fetchall()
        #     if not rows:
        #         try:
        #             last_obj = mapper_instance.send(None)
        #         except StopIteration:
        #             last_obj = None
        #         finally:
        #             mapper_instance.close()
        #             _cursor.close()
        #         if last_obj:
        #             yield last_obj
        #         break
        #     for row in rows:
        #         result = mapper_instance.send(row)
        #         if result is not None:
        #             yield result
        #             next(mapper_instance)

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


T = TypeVar('T')

@doublewrap
def in_transaction(fn: T, prop: str = 'db', type_: Type[Engine] = Engine) -> T:

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        with getattr(self, prop).transaction():
            return fn(self, *args, **kwargs)

    return add_extra_annotation(wrapper, prop, type_)
