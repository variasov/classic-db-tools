from types import TracebackType, ModuleType
from typing import Any, Optional, Type, ClassVar, Dict

from .pool import ConnectionPool
from .conn_scope import ConnectionScope
from .scope import Scope


class Transaction:
    implementations: ClassVar[Dict[ModuleType, Type['Transaction']]] = {}

    def __init_subclass__(cls, driver: ModuleType, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.implementations[driver] = cls

    def __init__(
            self,
            conn_pool: ConnectionPool,
            current: Scope,
            commit: bool = True,
            **kwargs,
    ):
        super().__init__()
        self._conn_pool = conn_pool
        self._commit_on_exit = commit
        self.conn_scope = None
        self.current = current
        self.params = kwargs

    def _at_enter(self, **kwargs: Any):
        raise NotImplemented

    def _at_exit(self):
        raise NotImplemented

    def __enter__(self):
        if self.current.tx_depth == 0:
            if self.current.conn is None:
                self.conn_scope = ConnectionScope(
                    self._conn_pool, self.current,
                )
                self.conn_scope.__enter__()

        try:
            if self.current.tx_depth == 0:
                self._at_enter()
                self.current.tx = self
                self.current.tx_depth += 1
            else:
                assert self.current.tx.params == self.params, (
                    'Transaction params do not match'
                )
        except Exception as exc:
            self.current.tx = None
            self.current.tx_depth = 0
            if self.conn_scope:
                self.conn_scope.__exit__(exc.__class__, exc, None)
            raise exc

    def __exit__(
            self,
            type_: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self.current.tx_depth -= 1
        if self.current.tx_depth != 0:
            return False

        try:
            if type_ is None and self._commit_on_exit:
                self.current.conn.commit()
            else:
                self.current.conn.rollback()

            self._at_exit()
        finally:
            if self.conn_scope:
                self.conn_scope.__exit__(type_, value, traceback)

            return False
