import contextlib
from types import TracebackType
from typing import Any, ClassVar, cast

from .conn_scope import ConnectionScope
from .dbapi import Connection, DBModule
from .pool import ConnectionPool
from .scope import Scope


class Transaction:
    implementations: ClassVar[dict[DBModule, type["Transaction"]]] = {}

    _conn_pool: ConnectionPool
    _commit_on_exit: bool
    _conn_scope: ConnectionScope | None
    _current: Scope
    _params: dict[str, Any]
    _first: bool | None

    def __init_subclass__(cls, driver: DBModule, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.implementations[driver] = cls

    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        conn.autocommit = True

    def __init__(
        self,
        conn_pool: ConnectionPool,
        current: Scope,
        commit: bool = True,
        params: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self._conn_pool = conn_pool
        self._commit_on_exit = commit
        self._conn_scope = None
        self._current = current
        self._params = params or {}
        self._first = None

    def _enable_params(self) -> None:
        raise NotImplementedError

    def _restore_params(self) -> None:
        raise NotImplementedError

    def _start_savepoint(self) -> None:
        raise NotImplementedError

    def _release_savepoint(self) -> None:
        raise NotImplementedError

    def _rollback_savepoint(self) -> None:
        conn = cast("Connection[Any]", self._current.conn)
        conn.rollback()

    def __enter__(self) -> None:
        if self._current.conn is None:
            self._conn_scope = ConnectionScope(
                self._conn_pool,
                self._current,
            )
            self._conn_scope.__enter__()

        try:
            if self._current.tx_params is None:
                self._current.tx_params = self._params
                self._first = True
                self._enable_params()
            else:
                assert self._current.tx_params == self._params, (
                    "Transaction params do not match"
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
            raise exc  # noqa: TRY201

    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        conn = cast("Connection[Any]", self._current.conn)
        first = self._first
        exc_info = (type_, value, traceback)
        try:
            try:
                if type_ is not None:
                    if first:
                        conn.rollback()
                    else:
                        self._rollback_savepoint()
                elif self._commit_on_exit:
                    if first:
                        conn.commit()
                    else:
                        self._release_savepoint()
                elif first:
                    conn.rollback()
                else:
                    self._rollback_savepoint()
            except BaseException as exc:
                exc_info = (type(exc), exc, exc.__traceback__)
                if first and type_ is None:
                    # Commit failed: the original error matters more
                    with contextlib.suppress(Exception):
                        conn.rollback()
                raise
            finally:
                if first:
                    try:
                        self._restore_params()
                    except Exception:
                        if exc_info[0] is None:
                            raise
        finally:
            if first:
                self._first = None
                self._current.tx_params = None

            if self._conn_scope:
                scope, self._conn_scope = self._conn_scope, None
                scope.__exit__(*exc_info)

        return False
