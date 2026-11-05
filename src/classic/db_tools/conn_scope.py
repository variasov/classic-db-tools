from types import TracebackType
from typing import cast

from .dbapi import Connection
from .pool import ConnectionPool
from .scope import Scope


class ConnectionScope:
    """
    Контекстный менеджер, обеспечивающий удержание коннекта из пула в scope
    при входе в контекст, и возврат коннекта в пул при выходе из контекста.

    Инстанцируется двжиком во время работы.
    """

    _conn_pool: ConnectionPool
    _current: Scope
    _first: bool | None

    def __init__(self, conn_pool: ConnectionPool, scope: Scope):
        super().__init__()
        self._conn_pool = conn_pool
        self._current = scope
        self._first = None

    def held_conn(self) -> Connection | None:
        """
        Возвращает соединение текущего Scope или None, если его нет.
        """
        return self._current.conn

    def acquire_detached(self) -> Connection:
        """
        Берет из пула отдельное соединение, не регистрируя его в Scope.
        Возвращать нужно через release_detached.
        """
        return cast("Connection", self._conn_pool.acquire())

    def release_detached(self, conn: Connection) -> None:
        self._conn_pool.release(conn)

    def __enter__(self) -> Connection:
        if self._current.conn is None:
            self._current.conn = self._conn_pool.acquire()
            self._first = True

        return cast("Connection", self._current.conn)

    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if self._first is True:
            try:
                self._conn_pool.release(self._current.conn)
            finally:
                self._first = None
                self._current.conn = None
        return False
