from collections.abc import Callable, Generator, Iterator
from functools import partial
from typing import (
    Any,
    Generic,
    cast,
)

from classic.db_tools.conn_scope import ConnectionScope
from classic.db_tools.dbapi import Cursor
from classic.db_tools.rows import Rows
from classic.db_tools.templates import Template

from .mapper import Mapper
from .params import Parameter, create_mapping
from .types import MapperFunc, Result

QueryParams = Any  # dict или объект с __dict__


class MapperQuery(Generic[Result]):
    """
    Объект-запрос с присоединенным маппером.

    Нужен для сложных преобразований результатов запросов
    в объекты и связанные коллекции объектов.
    """

    def __init__(
        self,
        conn_scope: Callable[[], ConnectionScope],
        tmpl_factory: Callable[[], Template],
        result: str,
        mapper: Mapper,
        params: dict[str, Parameter],
    ) -> None:
        self._conn_scope = conn_scope
        self._tmpl_factory = tmpl_factory
        self._result = result.lower()
        self._mapper = mapper
        self._mapping = create_mapping(**params)

    def sources(
        self,
        params: QueryParams = None,
        /,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> str:
        """
        Возвращает тело функции для скомпилированной функции-маппера
        под указанные параметры.

        Нужен для отладки.
        """

        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._sources(cursor, prepared)

        with self._conn_scope() as conn:
            return self._sources(conn.cursor(), prepared)

    def _sources(self, cursor: Cursor[Any], params: dict[str, Any]) -> str:
        mapper = self._execute(cursor, params)
        return getattr(mapper, "sources", lambda: "")()

    def all(
        self,
        params: QueryParams | None = None,
        /,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> list[Result]:
        """
        Возвращает список результатов запроса в виде заказанных объектов.

        Параметр cursor позволяет передать кастомный cursor.

        Нужен для случаев, когда необходимо получить список целиком,
        и выборка не очень велика, либо для случая, когда строки из запроса
        возвращаются неотстортированными, и нужно убедиться, что все отношения
        между объектами удовлетворены.
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
    ) -> list[Result]:
        mapper = self._execute(cursor, params)
        return list(mapper(iter(cursor.fetchall())))

    def iter(
        self,
        params: QueryParams | None = None,
        /,
        batch: int | None = 500,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> Rows[Result]:
        """
        Возвращает итератор Rows, возвращающий по одному объекту
        с его отношениями за раз.

        SQL выполняется сразу при вызове, а не при первом next().

        Внутри engine.conn()/engine.transaction() используется соединение
        из scope. Вне scope итератор держит собственное соединение из пула
        до исчерпания строк, вызова close() или выхода из блока with.
        Рекомендуется использовать with:

            with query.iter() as objects:
                ...

        Перебирает результаты запроса батчами указанного размера.

        Параметр cursor позволяет передать кастомный cursor.

        Нужен для случаев, когда строки из запроса возвращаются
        сгруппированными по корневому объекту, и необходимо перебрать объекты,
        экономя память.
        """

        prepared = self._prepare(params, kwargs)

        return Rows[Result].start(
            self._conn_scope,
            lambda cur: self._iter(cur, prepared, batch),
            cursor,
        )

    def _iter(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
        batch: int | None,
    ) -> Iterator[Result]:
        mapper = self._execute(cursor, params)
        return iter(mapper(self._iterate_rows(cursor, batch)))

    def one(
        self,
        params: QueryParams | None = None,
        /,
        batch: int = 500,
        cursor: Cursor[Any] | None = None,
        **kwargs: Any,
    ) -> Result | None:
        """
        Возвращает один объект с его отношениями.

        Перебирает результаты запроса батчами указанного размера.

        Параметр cursor позволяет передать кастомный cursor.

        Нужен для случаев, когда строки из запроса возвращаются
        сгруппированными по корневому объекту,
        и необходимо получить лишь один корневой объект.
        """

        prepared = self._prepare(params, kwargs)

        if cursor is not None:
            return self._one(cursor, prepared, batch)

        with self._conn_scope() as conn:
            return self._one(conn.cursor(), prepared, batch)

    def _one(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
        batch: int | None,
    ) -> Result | None:
        mapper = self._execute(cursor, params)
        iterator = mapper(self._iterate_rows(cursor, batch))
        try:
            result = next(iterator)
        except StopIteration:
            result = None
        finally:
            iterator.close()
        return result

    @staticmethod
    def _prepare(
        params: QueryParams,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        if params is None:
            return kwargs
        if isinstance(params, dict):
            return cast("dict[str, Any]", params)
        return params.__dict__

    def _execute(
        self,
        cursor: Cursor[Any],
        params: dict[str, Any],
    ) -> MapperFunc[Result]:
        self._tmpl_factory().execute(cursor, params)
        return self._mapper.func_for_cursor(
            cursor,
            self._mapping,
            self._result,
        )

    @staticmethod
    def _iterate_rows(
        cursor: Cursor[Any],
        batch: int | None,
    ) -> Generator[Any, None, None]:
        fetch = partial(cursor.fetchmany, batch) if batch else cursor.fetchall
        while True:
            rows = fetch()
            if not rows:
                return
            yield from rows
