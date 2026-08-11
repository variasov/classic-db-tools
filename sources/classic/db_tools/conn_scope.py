from types import TracebackType
from typing import Optional, Type, cast

from .scope import Scope
from .pool import ConnectionPool
from .dbapi import Connection


class ConnectionScope:
    """
    Контекстный менеджер, обеспечивающий удержание коннекта из пула в scope
    при входе в контекст, и возврат коннекта в пул при выходе из контекста.

    Инстанцируется двжиком во время работы.
    """

    _conn_pool: ConnectionPool
    _current: Scope
    _first: Optional[bool]

    def __init__(self, conn_pool: ConnectionPool, scope: Scope):
        super().__init__()
        self._conn_pool = conn_pool
        self._current = scope
        self._first = None

    def __enter__(self) -> Connection:
        if self._current.conn is None:
            self._current.conn = self._conn_pool.acquire()
            self._first = True

        return cast(Connection, self._current.conn)

    def __exit__(
            self,
            type_: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if self._first is True:
            try:
                self._conn_pool.release(self._current.conn)
            finally:
                self._first = None
                self._current.conn = None
        return False
