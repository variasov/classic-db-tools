from types import TracebackType
from typing import Callable, Generic, Iterator, Optional, Type, TypeVar

from .conn_scope import ConnectionScope
from .dbapi import Cursor


T = TypeVar('T')


class Rows(Generic[T], Iterator[T]):
    """
    Итератор по результатам запроса.

    SQL выполняется сразу при создании объекта (см. Rows.start),
    поэтому ошибки запроса возникают в момент вызова iter()/scalars(),
    а не на первом next().

    Если запрос выполнен вне engine.conn()/engine.transaction(),
    итератор удерживает собственное соединение из пула, не регистрируя его
    в Scope. Соединение возвращается в пул ровно один раз: при исчерпании
    строк, при вызове close(), при выходе из блока with или (как страховка)
    при удалении объекта.

    Если соединение уже есть в Scope, используется оно и не освобождается.
    """

    _closed = True
    _source: Optional[Iterator[T]] = None
    _cursor: Optional[Cursor] = None
    _release: Optional[Callable[[], None]] = None

    def __init__(
        self,
        source: Iterator[T],
        cursor: Optional[Cursor] = None,
        release: Optional[Callable[[], None]] = None,
    ) -> None:
        self._source = source
        self._cursor = cursor
        self._release = release
        self._closed = False

    @classmethod
    def start(
        cls,
        conn_scope: Callable[[], ConnectionScope],
        run: Callable[[Cursor], Iterator[T]],
        cursor: Optional[Cursor] = None,
    ) -> 'Rows[T]':
        """
        Выполняет запрос и возвращает Rows.

        run получает курсор, выполняет в нем запрос и возвращает итератор
        по результатам. Если передан пользовательский cursor, он используется
        как есть и не закрывается.
        """

        if cursor is not None:
            return cls(run(cursor))

        scope = conn_scope()
        conn = scope.held_conn()
        release: Optional[Callable[[], None]] = None
        if conn is None:
            conn = scope.acquire_detached()
            release = lambda: scope.release_detached(conn)  # noqa: E731

        own_cursor: Optional[Cursor] = None
        try:
            own_cursor = conn.cursor()
            return cls(run(own_cursor), own_cursor, release)
        except BaseException:
            try:
                if own_cursor is not None:
                    try:
                        own_cursor.close()
                    except Exception:
                        pass
            finally:
                if release is not None:
                    release()
            raise

    def __iter__(self) -> 'Rows[T]':
        return self

    def __next__(self) -> T:
        if self._closed or self._source is None:
            raise StopIteration
        try:
            return next(self._source)
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> 'Rows[T]':
        return self

    def __exit__(
        self,
        type_: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Закрывает курсор и возвращает соединение в пул. Идемпотентен.
        """

        if self._closed:
            return
        self._closed = True
        source, cursor, release = self._source, self._cursor, self._release
        self._source = self._cursor = self._release = None
        try:
            try:
                closer = getattr(source, 'close', None)
                if closer is not None:
                    closer()
            finally:
                if cursor is not None:
                    cursor.close()
        finally:
            if release is not None:
                release()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
