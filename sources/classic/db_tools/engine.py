from functools import wraps
import logging
from os import PathLike
from types import ModuleType
from typing import Any, Union, Sequence, Callable, Optional, Type
from pathlib import Path

from frozendict import frozendict

from .mapping import Mapper, Parameter, Mapping
from .pool import ConnectionPool
from .types import Connection
from .transaction import Transaction
from .conn_scope import ConnectionScope
from .scope import Scope
from .query import Query
from .templates import StaticTemplatesCache, DynamicTemplatesCache


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
        self.pool = pool_class(driver, self.factory, **(pool_kwargs or {}))
        self.current = Scope()
        self.tx_cls = Transaction.implementations[self.driver]

        if default_mapping:
            self.mapper = Mapper(default_mapping)
        else:
            self.mapper = frozendict()

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

        self.dynamic_templates = DynamicTemplatesCache(
            self.logger,
            templates_paths=self.templates_paths,
            paramstyle=self.driver.paramstyle,
            identifier_quote_char=identifier_quote_char,
        )
        self.static_templates = StaticTemplatesCache(
            self.logger,
            templates_paths=self.templates_paths,
        )
        self.mapper = Mapper(default_mapping)
        self.str_templates_static_by_default = str_templates_static_by_default

    def query_from(self, filename: str) -> Query:
        if filename.endswith('.sql'):
            tmpl = self.static_templates.get_or_create
        elif filename.endswith('.sql.tmpl'):
            tmpl = self.dynamic_templates.get_or_create
        else:
            raise ValueError(f'Unsupported filename extension: {filename}')
        return Query(self.conn, self.mapper, lambda: tmpl(filename=filename))

    def query(self, content: str, static: bool = None) -> Query:
        if static is None:
            static = self.str_templates_static_by_default

        if static is True:
            tmpl = self.static_templates.get_or_create
        elif static is False:
            tmpl = self.dynamic_templates.get_or_create
        else:
            raise ValueError(f'Unknown "static" arg value: {static}')

        return Query(self.conn, self.mapper, lambda: tmpl(content=content))

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
            return self.tx_cls(self.pool, self.current, commit, tx_kwargs)

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
