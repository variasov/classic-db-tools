from itertools import chain
from types import TracebackType
from typing import Callable, Any, Iterable, Generator, TypeAlias, Sequence, \
    Generic, Type, TypeVar
import threading
from pathlib import Path

from .dynamic.query import DynamicQuery
from .pool import ConnectionPool
from .static import StaticQuery
from .types import Connection, Cursor, CursorParams
from .params_styles import recognize_param_style

from . import dynamic, static, mapping

Query: TypeAlias = StaticQuery | DynamicQuery


class Engine(threading.local):

    def __init__(
            self,
            templates_path: str | Path,
            pool: ConnectionPool,
            identifier_quote_char: str = "'",
    ):
        self.pool = pool
        self.conn = None
        self.param_style = self._recognize_param_style()
        self.templates_path = templates_path
        self.dynamic_templates = dynamic.DynamicQueriesFactory(
            templates_path=templates_path,
            param_style=self.param_style,
            identifier_quote_char=identifier_quote_char,
        )
        self.static_templates = static.StaticQueriesFactory(templates_path)
        self.mapper_cache = {}

    def _recognize_param_style(self):
        with self:
            return recognize_param_style(self.conn)

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

    def from_str(self, query: str, static: bool = False) -> 'LazyQuery':
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
        return Transaction(self.conn)

    def __enter__(self):
        if self.conn is None:
            self.conn = self.pool.getconn()
        return self

    def __exit__(
            self,
            type_: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        if self.conn is None:
            return
        if self.conn.autocommit is False:
            if type_ is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        self.conn = self.pool.release(self.conn)
        self.conn = None
        self._autocommit = None
        return False

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


class Transaction:

    def __init__(self, conn: Connection):
        self.conn = conn

    def __enter__(self):
        self.return_autocommit_initial = self.conn.autocommit
        if self.conn.autocommit is True:
            self.conn.autocommit = False
        return self

    def __exit__(
            self,
            type_: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        if type_ is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        if self.return_autocommit_initial:
            self.conn.autocommit = True
        return False


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

    def return_as(self, *params: mapping.Param) -> 'LazyMapper':
        return LazyMapper(
            engine=self.engine,
            query_factory=self.query_factory,
            mapper_params=params,
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
        _raising: bool = False,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Any:
        _cursor = self.query.execute(
            params or kwargs,
            _cursor or self.engine.cursor,
        )
        value = _cursor.fetchone()
        if _raising and value is None:
            raise ValueError
        else:
            return value

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

    def rows(
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


Result = TypeVar('Result')


class LazyMapper(Generic[Result]):

    def __init__(
        self,
        engine: Engine,
        query_factory: Callable[[], Query],
        mapper_params: Iterable[mapping.Param],
    ) -> None:
        self.engine = engine
        self.query_factory = query_factory
        self.mapper_params = mapper_params
        self._query = None
        self._mapper = None

    @property
    def query(self) -> Query:
        query = self._query
        if query is None:
            query = self._query = self.query_factory()
        return query

    def mapper(self, cursor: Cursor) -> Generator[Any, Any, None]:
        columns = tuple(column[0] for column in cursor.description)
        key = tuple(chain(self.mapper_params, columns))
        mapper = self.engine.mapper_cache.get(key)
        if not mapper:
            mapper = mapping.compile_mapper(self.mapper_params, columns)
            self.engine.mapper_cache[key] = mapper
        return mapper()

    def all(
        self,
        params: CursorParams = None,
        /,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Iterable[Result]:
        return list(self.iter(params or kwargs, _cursor=_cursor))

    def iter(
        self,
        params: CursorParams = None,
        /,
        _batch: int | None = 500,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Generator[Result, None, None]:
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

    def one_or_none(
        self,
        params: CursorParams = None,
        /,
        _batch: int = 500,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Result:
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

    def one(
        self,
        params: CursorParams = None,
        /,
        _batch: int = 500,
        _cursor: Cursor = None,
        **kwargs: Any,
    ) -> Result:
        result = self.one_or_none(
            params or kwargs,
            _batch,
            _cursor or self.engine.cursor,
        )
        if result is None:
            raise ValueError('No result')
        return result
