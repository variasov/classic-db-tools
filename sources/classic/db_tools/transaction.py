from types import TracebackType, ModuleType
from typing import Any, Optional, Type, ClassVar, Dict

from .pool import ConnectionPool
from .conn_scope import ConnectionScope
from .scope import Scope


class Transaction:
    implementations: ClassVar[
        Dict[ModuleType, Type['Transaction']]
    ] = {}

    def __init_subclass__(cls, driver: ModuleType, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.implementations[driver] = cls

    def __init__(
            self,
            conn_pool: ConnectionPool,
            current: Scope,
            commit: bool = True,
            params: dict[str, Any] = None,
    ):
        super().__init__()
        self._conn_pool = conn_pool
        self._commit_on_exit = commit
        self._conn_scope = None
        self._current = current
        self._params = params or {}
        self._first = None

    def _enable_params(self):
        raise NotImplemented

    def _restore_params(self):
        raise NotImplemented

    def _start_savepoint(self):
        raise NotImplemented

    def _release_savepoint(self):
        raise NotImplemented

    def __enter__(self):
        if self._current.conn is None:
            self._conn_scope = ConnectionScope(
                self._conn_pool, self._current,
            )
            self._conn_scope.__enter__()

        try:
            if self._current.tx_params is None:
                self._current.tx_params = self._params
                self._first = True
                self._enable_params()
            else:
                assert self._current.tx_params == self._params, (
                    'Transaction params do not match'
                )
                self._first = False
                self._start_savepoint()
        except Exception as exc:
            if self._first:
                self._current.tx_params = None
                self._first = None

            if self._conn_scope:
                self._conn_scope.__exit__(exc.__class__, exc, None)
                self._conn_scope = None
            raise exc

    def __exit__(
            self,
            type_: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        try:
            if type_ is None:
                if self._first:
                    if self._commit_on_exit:
                        self._current.conn.commit()
                    else:
                        self._current.conn.rollback()
                else:
                    self._release_savepoint()
            else:
                self._current.conn.rollback()
            self._restore_params()
        finally:
            if self._first:
                self._first = None
                self._current.tx_params = None

            if self._conn_scope:
                self._conn_scope.__exit__(type_, value, traceback)

            return False
