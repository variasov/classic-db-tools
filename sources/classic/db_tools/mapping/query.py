from functools import partial
from typing import (
    Any, Generator, Generic, Dict, TypeVar, Type,
    Callable, ContextManager, Sequence, Union, cast,
)

from ..dbapi import Connection, Cursor, Row
from ..templates import Template

from .mapper import Mapper
from .params import Parameter, create_mapping
from .types import MapperFunc


QueryParams = Union[Dict[str, Any], Any]
Result = TypeVar('Result', bound=Type[object])


class MapperQuery(Generic[Result]):
    """
    Объект-запрос с присоединенным маппером.

    Нужен для сложных преобразований результатов запросов
    в объекты и связанные коллекции объектов.
    """

    def __init__(
        self,
        conn_scope: Callable[[], ContextManager[Connection]],
        tmpl_factory: Callable[[], Template],
        result: str,
        mapper: Mapper,
        params: Dict[str, Parameter],
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
        cursor: Cursor | None = None,
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

    def _sources(self, cursor: Cursor, params: Dict[str, Any]) -> str:
        mapper = self._execute(cursor, params)
        return getattr(mapper, 'sources', lambda: '')()

    def all(
        self,
        params: QueryParams | None = None,
        /,
        cursor: Cursor | None = None,
        **kwargs: Any,
    ) -> Sequence[Result]:
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

    def _all(self, cursor: Cursor, params: Dict[str, Any]) -> Sequence[Result]:
        mapper = self._execute(cursor, params)
        return list(mapper(iter(cursor.fetchall())))

    def iter(
        self,
        params: QueryParams | None = None,
        /,
        batch: int | None = 500,
        cursor: Cursor | None = None,
        **kwargs: Any,
    ) -> Generator[Result, None, None]:
        """
        Возвращает итератор, возвращающий по одному объекту с его отношениями за раз.

        Перебирает результаты запроса батчами указанного размера.

        Параметр cursor позволяет передать кастомный cursor.

        Нужен для случаев, когда строки из запроса возвращаются
        сгруппированными по корневому объекту, и необходимо перебрать объекты,
        экономя память.
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
        batch: int | None,
    ) -> Generator[Result, None, None]:
        mapper = self._execute(cursor, params)
        for obj in mapper(self._iterate_rows(cursor, batch)):
            yield obj

    def one(
        self,
        params: QueryParams | None = None,
        /,
        batch: int = 500,
        cursor: Cursor | None = None,
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
        cursor: Cursor,
        params: Dict[str, Any],
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
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        if params is None:
            return kwargs
        if isinstance(params, dict):
            return cast(Dict[str, Any], params)
        else:
            return params.__dict__

    def _execute(
        self,
        cursor: Cursor,
        params: Dict[str, Any],
    ) -> MapperFunc:
        self._tmpl_factory().execute(cursor, params)
        return self._mapper.func_for_cursor(
            cursor, self._mapping, self._result,
        )

    @staticmethod
    def _iterate_rows(
        cursor: Cursor,
        batch: int | None,
    ) -> Generator[
        Row, None, None,
    ]:
        if batch:
            fetch = partial(cursor.fetchmany, batch)
        else:
            fetch = cursor.fetchall
        while True:
            rows = fetch()
            if not rows:
                return
            for row in rows:
                yield row
