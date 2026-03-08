from functools import wraps, partial
from os import PathLike
from types import TracebackType
from typing import (
    Any, Iterable, Generator, Union, List,
    Sequence, Generic, Hashable,
    Type, TypeVar, Callable, Optional,
)
import threading
from pathlib import Path

from .mapping import Result, Mapper, MapperFunc, compile_mapper_func
from .doublewrap import doublewrap
from .pool import ConnectionPool
from .types import Cursor, CursorParams, Row
from .transaction import Transaction
from .scoped_connection import ScopedConnection

from . import dynamic, static


QueryParams = Union[CursorParams, Any]


class Engine:

    def __init__(
        self,
        templates_paths: Union[str, PathLike, Sequence[Union[str, PathLike]]],
        pool: ConnectionPool,
        mapper: Optional[Mapper] = None,
        commit_on_exit: bool = True,
        str_templates_static_by_default: bool = False,
        identifier_quote_char: str = "'",
    ):
        self.pool = pool
        self.conn = ScopedConnection(pool, commit_on_exit)
        self.mapper = mapper
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
            type_: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> Optional[bool]:
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

    def map_to(
        self,
        result: Result = None,
        prefix: Optional[str] = None,
        *,
        mapper: Optional[Mapper] = None,
    ) -> 'MappedQuery[Result]':
        if prefix is None:
            if result is None:
                raise ValueError('Prefix or result must be specified')
            prefix_ = result.__name__.lower()
        else:
            prefix_ = prefix.lower()
        return MappedQuery[Result](
            engine=self.engine,
            lazy_query=self._lazy_query,
            result=prefix_,
            mapper=mapper or self.engine.mapper,
        )

    def execute(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ) -> Cursor:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        return self._lazy_query().execute(
            params or kwargs,
            cursor or self.engine.cursor,
        )

    def executemany(
        self,
        params: Iterable[QueryParams],
        cursor: Cursor = None,
    ) -> Cursor:
        return self._lazy_query().executemany([
            param
            if isinstance(param, (dict, tuple))
            else param.__dict__
            for param in params
        ], cursor or self.engine.cursor,)

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor = None,
        **kwargs: Any,
    ):
        if params is None:
            params = kwargs
        else:
            if not isinstance(params, (dict, tuple)):
                params = params.__dict__

        cursor = self._lazy_query().execute(
            params,
            cursor or self.engine.cursor,
        )
        return cursor.fetchall()

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch_: int = 500,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        cursor_ = self._lazy_query().execute(
            params or kwargs,
            cursor_ or self.engine.cursor,
        )
        while True:
            batch = cursor_.fetchmany(batch_)
            if not batch:
                return
            for row in batch:
                yield row

    def one(
        self,
        params: QueryParams = None,
        /,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        cursor_ = self._lazy_query().execute(
            params or kwargs,
            cursor_ or self.engine.cursor,
        )
        return cursor_.fetchone()

    def scalar(
        self,
        params: QueryParams = None,
        /,
        raising_: bool = False,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        row = self.one(
            params or kwargs,
            raising_=raising_,
            cursor_=cursor_ or self.engine.cursor,
        )
        if not raising_ and row is None:
            return None
        return row[0]

    def scalars(
        self,
        params: QueryParams = None,
        /,
        raising_: bool = False,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        return (
            row[0] for row in self.iter(
                params or kwargs,
                raising_=raising_,
                cursor_=cursor_ or self.engine.cursor,
            )
        )

    def rowcount(
        self,
        params: QueryParams = None,
        /,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> int:
        """Количество строк, обработанных запросом"""

        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        cursor = self._lazy_query().execute(
            params or kwargs,
            cursor_ or self.engine.cursor,
        )
        return cursor.rowcount


class MappedQuery(Generic[Result]):

    def __init__(
        self,
        engine: Engine,
        lazy_query,
        result: str,
        mapper: Mapper,
    ) -> None:
        self.engine = engine
        self._lazy_query = lazy_query
        self.result = result.lower()
        self.mapper = mapper
        self._mapper = None

        # Alias for test simplicity
        self._compile_mapper = compile_mapper_func

    def mapper_func(self, cursor: Cursor) -> Callable[
        [Iterable[Row]],
        Generator[Any, Any, None]
    ]:
        columns = tuple(column[0] for column in cursor.description)
        key = (self.result, self.mapper, columns)
        mapper = self.engine.get_mapper_from_cache(key)
        if not mapper:
            mapper = self._compile_mapper(
                self.result, self.mapper, columns,
            )
            self.engine.cache_mapper(key, mapper)
        return mapper

    def sources(
        self,
        params: QueryParams = None,
        /,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> str:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        cursor = self._lazy_query().execute(
            params or kwargs,
            cursor_ or self.engine.cursor,
        )
        func = self.mapper_func(cursor)
        return getattr(func, 'sources', lambda: '')()

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> List[Result]:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        return list(self.iter(params or kwargs, cursor_=cursor_))

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: Optional[int] = 500,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Result, None, None]:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        cursor_ = self._lazy_query().execute(
            params or kwargs,
            cursor_ or self.engine.cursor,
        )
        mapper = self.mapper_func(cursor_)

        if batch:
            fetch = partial(cursor_.fetchmany, batch)
        else:
            fetch = cursor_.fetchall

        def rows_iter():
            while True:
                rows = fetch()
                if not rows:
                    return
                for row in rows:
                    yield row

        for obj in mapper(rows_iter()):
            yield obj

    def one(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor_: Cursor = None,
        **kwargs: Any,
    ) -> Result:
        if params is not None and not isinstance(params, (dict, tuple)):
            params = params.__dict__

        iterator = self.iter(
            params or kwargs,
            batch,
            cursor_ or self.engine.cursor,
        )
        try:
            result = next(iterator)
        except StopIteration:
            iterator.close()
            result = None
        return result


T = TypeVar('T')

@doublewrap
def in_transaction(fn: T, prop: str = 'db') -> T:

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        with getattr(self, prop).transaction():
            return fn(self, *args, **kwargs)

    return wrapper
