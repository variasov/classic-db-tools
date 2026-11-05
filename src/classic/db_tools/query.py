from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import (
    Any,
    Generic,
    cast,
)

from .conn_scope import ConnectionScope
from .dbapi import Cursor, RowT
from .detached_cursor import DetachedCursor
from .mapping import Mapper, MapperQuery, Parameter
from .mapping.types import Result
from .rows import Rows
from .templates import Template

QueryParams = Any  # dict, tuple или объект с __dict__


class Query(Generic[RowT]):
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
    ) -> None:
        self._conn_scope = conn_scope
        self._mapper = mapper
        self._tmpl_factory = tmpl_factory

    def map_to(
        self,
        result: Callable[..., Result],
        prefix: str | None = None,
        /,
        **params: Parameter,
    ) -> MapperQuery[Result]:
        """
        Возвращает объект MapperQuery с указанными параметрами.

        result - класс или функция-фабрика объектов результата.
        """

        prefix_ = result.__name__.lower() if prefix is None else prefix.lower()
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
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> Cursor[RowT]:
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает курсор. Внутри engine.conn()/engine.transaction()
        это настоящий курсор драйвера. Вне scope соединение возвращается
        в пул сразу после выполнения, а результатом будет DetachedCursor,
        у которого доступен только rowcount; остальные обращения
        вызывают CursorOutsideScopeError.

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

        return self._run_detachable(
            lambda cur: self._tmpl_factory().execute(cur, prepared),
        )

    def _run_detachable(
        self,
        run: Callable[[Cursor[Any]], Cursor[Any]],
    ) -> Cursor[RowT]:
        scope = self._conn_scope()
        in_scope = scope.held_conn() is not None
        with scope as conn:
            cur = conn.cursor()
            if in_scope:
                return run(cur)
            try:
                rowcount = run(cur).rowcount
            finally:
                cur.close()
        return cast("Cursor[RowT]", DetachedCursor(rowcount))

    def executemany(
        self,
        params: Iterable[QueryParams],
        cursor: Cursor[Any] | None = None,
    ) -> Cursor[RowT]:
        """
        Запускает множественное выполнение запроса с указанными параметрами.

        Возвращает курсор. Вне engine.conn()/engine.transaction() возвращает
        DetachedCursor с доступным только rowcount (см. execute).

        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Под капотом выполняет сборку шаблона для каждого экземпляра
        из переданных параметров и отправку с .execute.

        Нужен для случаев, когда необходимо отправить несколько запросов
        для набора параметров без работы с результатами.
        """

        prepared = cast(
            "list[dict[str, Any]]",
            [
                param if isinstance(param, (dict, tuple)) else param.__dict__
                for param in params
            ],
        )

        if cursor is not None:
            return self._tmpl_factory().executemany(cursor, prepared)

        return self._run_detachable(
            lambda cur: self._tmpl_factory().executemany(cur, prepared),
        )

    def all(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> Sequence[RowT]:
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

    def _all(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
    ) -> Sequence[RowT]:
        cursor = self._tmpl_factory().execute(cursor, params)
        return cursor.fetchall()

    def iter(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> Rows[RowT]:
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает итератор Rows, который вытаскивает батчи указанного
        размера из курсора, и возвращает по одному результату запроса.

        SQL выполняется сразу при вызове, а не при первом next().

        Внутри engine.conn()/engine.transaction() используется соединение
        из scope. Вне scope итератор держит собственное соединение из пула
        до исчерпания строк, вызова close() или выхода из блока with.
        Рекомендуется использовать with:

            with query.iter() as rows:
                ...

        Параметр batch позволяет задать размер батча.
        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо перебрать результаты,
        экономя память при больших выборках.
        """

        prepared = self._prepare(params, kwargs)

        return Rows[RowT].start(
            self._conn_scope,
            lambda cur: self._iter(cur, prepared, batch),
            cursor,
        )

    def _iter(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
        batch: int,
    ) -> Iterator[RowT]:
        cursor = self._tmpl_factory().execute(cursor, params)
        return self._fetch_rows(cursor, batch)

    def one(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> RowT | None:
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

    def _one(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
    ) -> RowT | None:
        cursor = self._tmpl_factory().execute(cursor, params)
        return cursor.fetchone()

    def scalar(
        self,
        params: QueryParams = None,
        /,
        raising: bool = False,
        cursor: Cursor[Any] | None = None,
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
        cursor: Cursor[Any],
        params: dict[str, Any],
        raising: bool,
    ) -> Any:
        cursor_ = self._tmpl_factory().execute(cursor, params)
        row = cursor_.fetchone()
        if row is None:
            if not raising:
                return None
            raise IndexError("scalar query returned no rows")
        return row[0]

    def scalars(
        self,
        params: QueryParams = None,
        /,
        batch: int = 500,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> Rows[Any]:
        """
        Запускает выполнение запроса с указанными параметрами.

        Возвращает итератор Rows, возвращающий по одному значения первого
        столбца из каждой строки.

        SQL выполняется сразу при вызове, а не при первом next().

        Внутри engine.conn()/engine.transaction() используется соединение
        из scope. Вне scope итератор держит собственное соединение из пула
        до исчерпания строк, вызова close() или выхода из блока with.
        Рекомендуется использовать with:

            with query.scalars() as values:
                ...

        Параметр batch позволяет задать размер батча.
        Параметр cursor позволяет передать кастомный cursor.

        При первом вызове провоцирует загрузку шаблона.
        Так же запускает сборку динамического шаблона перед выполнением.

        Нужен для случаев, когда необходимо получить список значений.
        """

        prepared = self._prepare(params, kwargs)

        return Rows[Any].start(
            self._conn_scope,
            lambda cur: self._scalars(cur, prepared, batch),
            cursor,
        )

    def _scalars(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
        batch: int,
    ) -> Iterator[Any]:
        cursor = self._tmpl_factory().execute(cursor, params)
        return (row[0] for row in self._fetch_rows(cursor, batch))

    @staticmethod
    def _fetch_rows(cursor: Cursor[Any], batch: int) -> Iterator[Any]:
        while True:
            batch_ = cursor.fetchmany(batch)
            if not batch_:
                return
            yield from batch_

    def rowcount(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor[Any] | None = None,
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
            return (
                self._tmpl_factory()
                .execute(
                    conn.cursor(),
                    prepared,
                )
                .rowcount
            )

    @staticmethod
    def _prepare(params: QueryParams, kwargs: dict[str, Any]) -> dict[str, Any]:
        if params is None:
            return kwargs
        if isinstance(params, dict):
            if kwargs:
                return {**params, **kwargs}
            return params
        if isinstance(params, tuple):
            return cast("dict[str, Any]", params)
        return {**params.__dict__, **kwargs}
