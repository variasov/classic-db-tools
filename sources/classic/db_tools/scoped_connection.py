import threading
from types import TracebackType
from typing import  Any

from .pool import ConnectionPool
from .types import Connection


class ScopedConnection(threading.local):
    _conn_pool: ConnectionPool

    def __init__(
            self,
            conn_pool: ConnectionPool,
            commit_on_exit: bool = True,
    ):
        super().__init__()
        self._conn_pool = conn_pool
        self._commit_on_exit = commit_on_exit
        self._started = False

    def __enter__(self) -> Connection:
        self._conn = self._conn_pool.getconn()
        self._started = True
        return self._conn

    def __exit__(
            self,
            type_: type[BaseException] | None,
            value: BaseException | None,
            traceback: TracebackType | None,
    ) -> bool | None:
        if not hasattr(self, '_conn'):
            return False

        if self._conn.autocommit is False:
            if type_ is None and self._commit_on_exit:
                self._conn.commit()
            else:
                self._conn.rollback()

        self._conn_pool.release(self._conn)
        del self._conn
        return False

    def __getattr__(self, item: str) -> Any:
        if not self._started:
            raise AttributeError(f'''
                Trying to access {item}, while not in started state.
                Maybe, you forgot to enter in ScopedConnection?:
                >>> with ScopedConnection(pool) as conn:
                ...     query.execute(conn)
            ''')
        return getattr(self._conn, item)

    @property
    def __wrapped__(self) -> Connection:
        return self._conn
