from functools import wraps
import logging
from os import PathLike
from types import ModuleType
from typing import (
    TypeVar, Any, ParamSpec, Union, Sequence,
    Callable, Optional, Type, cast, overload,
)
from pathlib import Path

from frozendict import frozendict

from .mapping import Mapper, Parameter, Mapping
from .pool import ConnectionPool
from .transaction import Transaction
from .conn_scope import ConnectionScope
from .scope import Scope
from .query import Query
from .templates import StaticTemplatesCache, DynamicTemplatesCache


Params = ParamSpec('Params')
Result = TypeVar('Result')


class Engine:
    templates_dirs: Sequence[Union[str, PathLike]]
    tx_cls: Type[Transaction]
    mapper: Mapper

    def __init__(
        self,
        driver: ModuleType,
        factory: Optional[Callable[[], Any]] = None,
        /,
        pool_class: Type[ConnectionPool] = ConnectionPool,
        pool_kwargs: Optional[Mapping] = None,
        templates_dirs: Optional[Union[
            str, PathLike, Sequence[Union[str, PathLike]],
        ]] = None,
        default_mapping: Optional[dict[str, Parameter]] = None,
        logger: Optional[logging.Logger] = None,
        str_templates_static_by_default: bool = False,
        identifier_quote_char: Optional[str] = None,
    ):
        self.driver = driver

        if factory is None:
            self.factory = driver.connect
        else:
            assert callable(self.factory)
            self.factory = factory

        self.pool_cls = pool_class
        self.pool = pool_class(driver, self.factory, **(pool_kwargs or {}))
        self.current = Scope()
        self.tx_cls = Transaction.implementations[self.driver]

        self.mapper = Mapper(frozendict(default_mapping or {}))

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
        self.str_templates_static_by_default = str_templates_static_by_default

    def query_from(self, filename: str) -> Query:
        if filename.endswith('.sql'):
            tmpl = self.static_templates.get_or_create
        elif filename.endswith('.sql.tmpl'):
            tmpl = self.dynamic_templates.get_or_create
        else:
            raise ValueError(f'Unsupported filename extension: {filename}')
        return Query(
            cast(Callable[[], ConnectionScope], self.conn),
            self.mapper,
            lambda: tmpl(filename=filename),
        )

    def query(self, content: str, static: Optional[bool] = None) -> Query:
        if static is None:
            static = self.str_templates_static_by_default

        if static is True:
            tmpl = self.static_templates.get_or_create
        elif static is False:
            tmpl = self.dynamic_templates.get_or_create

        return Query(
            cast(Callable[[], ConnectionScope], self.conn),
            self.mapper,
            lambda: tmpl(content=content),
        )

    @overload
    def transaction(
        self,
        fn: Callable[Params, Result],
        /,
        commit: bool = True,
        **tx_kwargs: Any,
    ) -> Callable[Params, Result]:
        ...

    @overload
    def transaction(
        self,
        fn: None = None,
        /,
        commit: bool = True,
        **tx_kwargs: Any,
    ) -> Transaction:
        ...

    def transaction(
        self,
        fn: Callable[Params, Result] | None = None,
        /,
        commit: bool = True,
        **tx_kwargs: Any,
    ) -> Callable[Params, Result] | Transaction:
        if fn is None:
            return self.tx_cls(self.pool, self.current, commit, tx_kwargs)
        else:
            @wraps(fn)
            def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Result:
                with self.transaction(commit=commit, **tx_kwargs):
                    return fn(*args, **kwargs)

            return wrapper

    @overload
    def conn(self, fn: Callable[Params, Result], /) -> Callable[Params, Result]:
        ...

    @overload
    def conn(self, fn: None = None, /) -> ConnectionScope:
        ...

    def conn(
        self,
        fn: Callable[Params, Result] | None = None,
        /,
    ) -> Callable[Params, Result] | ConnectionScope:
        if fn is None:
            return ConnectionScope(self.pool, self.current)

        @wraps(fn)
        def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Result:
            with self.conn(None):
                return fn(*args, **kwargs)

        return wrapper
