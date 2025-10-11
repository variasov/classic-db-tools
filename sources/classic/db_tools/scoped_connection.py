import threading
from types import TracebackType
from typing import  Any

from .pool import ConnectionPool
from .types import Connection


class ScopedConnection(threading.local):
    _conn_pool: ConnectionPool

    def __init__(self, conn_pool: ConnectionPool):
        super().__init__()
        self._conn_pool = conn_pool

    def __enter__(self) -> Connection:
        self._conn = self._conn_pool.getconn()
        return self._conn

    def __exit__(
            self,
            type_: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        self._conn_pool.release(self._conn)
        del self._conn
        return False

    def __getattr__(self, item: str) -> Any:
        return getattr(self._conn, item)

    @property
    def wrapped_conn_cls(self) -> str:
        return self._conn.__class__


class Transaction:

    def __init__(self, conn: Connection):
        self._conn = conn

    def __enter__(self) -> Connection:
        return self._conn

    def __exit__(
            self,
            type_: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        if value:
            self._conn.rollback()
        else:
            self._conn.commit()

        return False
