from types import TracebackType
from typing import Optional, Type

from .scope import Scope
from .pool import ConnectionPool
from .types import Connection


class ConnectionScope:
    _conn_pool: ConnectionPool
    current: Scope

    def __init__(self, conn_pool: ConnectionPool, scope: Scope):
        super().__init__()
        self._conn_pool = conn_pool
        self.current = scope

    def __enter__(self) -> Connection:
        if self.current.depth == 0:
            self.current.conn = self._conn_pool.getconn()
        self.current.depth += 1
        return self.current.conn

    def __exit__(
            self,
            type_: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self.current.depth -= 1
        if self.current.depth != 0:
            return False

        self._conn_pool.release(self.current.conn)
        self.current.conn = None
        return False
