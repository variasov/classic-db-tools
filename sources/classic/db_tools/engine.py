from functools import wraps
import logging
from os import PathLike
from types import ModuleType
from typing import (
    Dict, TypeVar, Any, ParamSpec, Union, Sequence,
    Callable, Optional, Type, cast, overload,
)
from pathlib import Path

from frozendict import frozendict

from .mapping import Mapper, Parameter
from .pool import ConnectionPool
from .transaction import Transaction
from .conn_scope import ConnectionScope
from .scope import Scope
from .query import Query
from .templates import StaticTemplatesCache, DynamicTemplatesCache


Params = ParamSpec('Params')
Result = TypeVar('Result')


class Engine:
    """
    Точка входа в библиотеку.

    Для доступа к разным инстансам БД следует использовать разные инстансы Engine.
    """

    current: Scope

    _templates_dirs: Sequence[Union[str, PathLike]]
    _tx_cls: Type[Transaction]
    _mapper: Mapper
    _factory: Callable[[], Any]
    _pool: ConnectionPool
    _pool_cls: Type[ConnectionPool]
    _logger: logging.Logger


    def __init__(
        self,
        driver: ModuleType,
        factory: Optional[Callable[[], Any]] = None,
        /,
        templates_dirs: Optional[Union[
            str, PathLike, Sequence[Union[str, PathLike]],
        ]] = None,
        str_templates_static_by_default: bool = False,
        identifier_quote_char: Optional[str] = None,
        default_mapping: Optional[dict[str, Parameter]] = None,
        pool_class: Type[ConnectionPool] = ConnectionPool,
        pool_kwargs: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Аргументы:

        driver: Модуль драйвера к БД, с которым будет происходить работа.
                Модуль должен соответствовать спецификации DB API 2.0 (PEP 249).

        factory: Опциональная функция-фабрика, порождающая подключение к БД.
                 При отсутствии будет использоваться функция connect из модуля.

        templates_dirs: Один или несколько путей до директорий,
                        содержащих шаблоны запросов.

        str_templates_static_by_default: Регулирует значение по умолчанию
                                         аргмуента static в методе Engine.query.

        identifier_quote_char: Символ кавычек, используемый для экранирования идентификаторов.
                               Зависит от БД, используемой движком. Может быть ', " и `.

        default_mapping: Конфигурация для маппера по умолчанию. Будет использоваться
                         методом Query.map_to, если при вызове map_to не переданы kwargs.

        pool_class: Класс пула соединений, используемый Engine.

        pool_kwargs: Параметры для пула соединений. Распаковывается в конструктор пула.

        logger: Кастомный объект logging.Logger для логгирования внутри библиотеки.
                Если не задан, используется логгер с именем 'classic-db-tools'
        """

        self.current = Scope()

        assert hasattr(driver, 'connect')
        self.driver = driver

        if factory is None:
            self._factory = driver.connect
        else:
            assert callable(factory)
            self._factory = factory

        self._pool_cls = pool_class
        self._pool = pool_class(driver, self._factory, **(pool_kwargs or {}))

        self._tx_cls = Transaction.implementations[self.driver]

        self._logger = logger or logging.getLogger('classic-db-tools')

        self._str_templates_static_by_default = str_templates_static_by_default

        self._mapper = Mapper(frozendict(default_mapping or {}))

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
            self._logger,
            templates_paths=self.templates_paths,
            paramstyle=self.driver.paramstyle,
            identifier_quote_char=identifier_quote_char,
        )
        self.static_templates = StaticTemplatesCache(
            self._logger,
            templates_paths=self.templates_paths,
        )

    def query_from(self, filename: str) -> Query:
        """
        Возвращает объект-запрос с шаблоном SQL, содержащимя по указанному пути.

        Поиск шаблона производится относительно директорий с шаблонами,
        переданных в конструктор Engine. Возвращается первый попавшийся.

        Статические шаблон должны иметь рашсирение .sql,
        динамический - .sql.tmpl.

        При этом объект Query сам по себе произведет загрузку содержимого шаблона
        не сразу, а при первом обращении к методам, провоцирующим исполнение.

        Нужен для работы с файловыми шаблонами SQL.
        """

        if filename.endswith('.sql'):
            tmpl = self.static_templates.get_or_create
        elif filename.endswith('.sql.tmpl'):
            tmpl = self.dynamic_templates.get_or_create
        else:
            raise ValueError(f'Unsupported filename extension: {filename}')
        return Query(
            cast(Callable[[], ConnectionScope], self.conn),
            self._mapper,
            lambda: tmpl(filename=filename),
        )

    def query(self, content: str, static: Optional[bool] = None) -> Query:
        """
        Возвращает объект-запрос с указанным шаблоном SQL.

        Параметр static указывает на тип шаблона.
        Если передаваемый шаблон содержит макросы Jinja, static должен быть False.

        Нужен для случаев, когда SQL сгенерирован динамически
        или указывается прямо в Python-коде.
        """
        if static is None:
            static = self._str_templates_static_by_default

        if static is True:
            tmpl = self.static_templates.get_or_create
        elif static is False:
            tmpl = self.dynamic_templates.get_or_create

        return Query(
            cast(Callable[[], ConnectionScope], self.conn),
            self._mapper,
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
        """
        Метод имеет 2 применения - как контекстный менеджер и как декоратор.

        При применении с with engine захватит соединение во внутреннем пуле
        соединений, убедится в отключении autocommit.
        При выходе из with вызовет conn.commit при commit=True, или rollback,
        если commit=False, или если возникли исключения в блоке with.

        При применении как декоратора вернет функцию-обертку, которая при вызове
        обернет в with engine.connect декорируемую функцию.

        Нужен для объявления границ транзакций.
        """
        if fn is None:
            return self._tx_cls(self._pool, self.current, commit, tx_kwargs)
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
        """
        Метод имеет 2 применения - как контекстный менеджер и как декоратор.

        При применении с with engine захватит соединение во внутреннем пуле
        соединений, и будет удерживать соединение до конца блока.

        При применении как декоратора вернет функцию-обертку, которая при вызове
        обернет в with engine.connect декорируемую функцию.

        Нужно для случаев, когда нужно сделать несколько запросов в одной операции подряд.
        """
        if fn is None:
            return ConnectionScope(self._pool, self.current)

        @wraps(fn)
        def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Result:
            with self.conn(None):
                return fn(*args, **kwargs)

        return wrapper
