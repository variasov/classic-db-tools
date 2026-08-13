from typing import (
    Any, Iterable, Generator, List, Sequence, Union, Dict,
    Callable, Optional, cast,
)

from .mapping import Mapper, Result, Parameter, MapperQuery
from .dbapi import Row, Cursor
from .templates import Template
from .conn_scope import ConnectionScope


QueryParams = Union[Dict[str, Any], Any]


class Query:
    """
    Объект-запрос.

    Предоставляет методы для коммуникации с БД и преобразования результатов.

    Загрузка шаблона запроса происходит при первом вызове любого
    метода для коммуникации.

    Инстанцируется движком, напрямую инстанцироваться
    в пользовательском коде он не должен.
    """

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
        """
        Возвращает объект MapperQuery с указанными параметрами.
        """

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
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает курсор.

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо сделать что-то кастомное
        с курсором, чего не умеет Engine, либо для случаев, когда
        вызывающую программу не интересует результат.
        """

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
        """
        Запускает множественное выполнение запроса с указанными параметрами.

        Возвращает курсор.

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Под капотом выполняет сборку шаблона для каждого экземпляра
        из переданных параметров и отправку с .execute.

        Нужен для случаев, когда необходимо отправить несколько запросов
        для набора параметров без работы с результатами.
        """

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
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает результат запроса в виде списка объектов.

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо получить результат запроса целиком.
        """

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
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает итератор, который вытаскивает батчи указанного размера из курсора,
        и возвращает по одному результату запроса.

        Параметр batch позволяет задать размер батча.
        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо перебрать результаты,
        экономя память при больших выборках.
        """

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
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает первую строку из результатов или None, если результат пустой.

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо получить одну строку из результатов.
        """

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
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает значение первого столбца из первой строки результатов
        или None, если результат пустой.

        При raising=True вызовет исключение IndexError вместо возврата None.

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо получить одно единственное значение.
        """

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
        if row is None:
            if not raising:
                return None
            raise IndexError('scalar query returned no rows')
        return row[0]

    def scalars(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor | None = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает итератор, возвращающий по одному значения первого столбца
        из каждой строки.

        Параметр batch позволяет задать размер батча.
        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо получить список значений.
        """

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
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает число строк, обработанных запросом

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо результаты запроса не интересуют,
        а только число задетых строк, например, для логгирования.
        """

        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._tmpl_factory().execute(cursor, prepared).rowcount

        with self._conn_scope() as conn:
            return self._tmpl_factory().execute(
                conn.cursor(), prepared,
            ).rowcount

    @staticmethod
    def _prepare(params: QueryParams, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        params_: Dict[str, Any] = params
        if params is None:
            params_ = kwargs
        elif not isinstance(params, (dict, tuple)):
            params_ = params.__dict__
        return params_
