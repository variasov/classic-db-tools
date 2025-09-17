import os.path
from types import SimpleNamespace, TracebackType
from typing import Callable, Optional
import threading
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template

from .templates import Renderer, AutoBind
from .pool import ConnectionPool
from .types import Connection
from .result import Result
from .params_styles import recognize_param_style


class Engine(threading.local):
    VALID_ID_QUOTE_CHARS = ('`', "'")
    VALID_PARAM_STYLES = (
        'qmark',     # qmark 'where name = ?'
        'numeric',   # numeric 'where name = :1'
        'named',     # named 'where name = :name'
        'format',    # format 'where name = %s'
        'pyformat',  # pyformat 'where name = %(name)s'
        'asyncpg',   # asyncpg 'where name = $1'
    )

    def __init__(
            self,
            conn_factory: Callable[[], Connection],
            templates_path: str | Path,
            pool_size: int = 1,
            pool_timeout: int = 5,
            auto_reload: bool = False,
            identifier_quote_character: str = "'",
    ):
        assert identifier_quote_character in self.VALID_ID_QUOTE_CHARS
        self.identifier_quote_character = identifier_quote_character

        self.renderer = Renderer()

        self.templates = Environment(
            loader=FileSystemLoader(templates_path),
            auto_reload=auto_reload,
            autoescape=True,
        )
        self.templates.add_extension(AutoBind)
        self.templates.filters['bind'] = self.renderer.bind
        self.templates.filters['sqlsafe'] = self.renderer.sql_safe
        self.templates.filters['inclause'] = self.renderer.bind_in_clause
        self.templates.filters['identifier'] = (
            self.renderer.build_escape_identifier_filter(
                self.identifier_quote_character,
            )
        )
        self.queries = self._load_queries(self.templates.list_templates())
        self.pool = ConnectionPool(
            conn_factory,
            limit=pool_size,
            timeout=pool_timeout,
        )
        self.conn = None
        self.mapper_cache = {}

    def _load_queries(self, paths: list[str]):
        queries = SimpleNamespace()
        for template_path in paths:
            chunks = template_path.split('/')

            last = queries
            for chunk in chunks[:-1]:
                new = getattr(last, chunk, None)
                if new is None:
                    new = SimpleNamespace()

                setattr(last, chunk, new)
                last = new

            setattr(
                last,
                os.path.splitext(chunks[-1])[0],
                self.from_file(template_path)
            )
        return queries

    def from_file(self, filename: str) -> 'Query':
        template = self.templates.get_template(filename)
        return Query(template, self)

    def from_str(self, query: str) -> 'Query':
        template = self.templates.from_string(query)
        return Query(template, self)

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


class Query:

    def __init__(
        self,
        template: Template,
        engine: Engine,
    ):
        self.template = template
        self.sql = None
        self.parameters = None
        self.engine = engine

    def execute(
        self,
        batch_params: Optional[list[object]] = None,
        /,
        **kwargs: object,
    ) -> Result:
        connection = self.engine.conn
        param_style = recognize_param_style(connection)

        sql, ordered_params = self.engine.renderer.prepare_query(
            self.template, param_style, kwargs or {},
        )

        cursor = self.engine.conn.cursor()

        if batch_params is not None:
            assert not kwargs, ('Only batch_params or kwargs '
                                'allowed at the same time')
            cursor.executemany(sql, batch_params)
        else:
            cursor.execute(sql, ordered_params)

        return Result(cursor, self.engine.mapper_cache)

    __call__ = execute
