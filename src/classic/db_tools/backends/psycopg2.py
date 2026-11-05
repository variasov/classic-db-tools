from typing import Any, Protocol, cast

import psycopg2

from classic.db_tools.conn_validator import ConnectionValidator
from classic.db_tools.dbapi import Connection
from classic.db_tools.transaction import Transaction

TRANSACTION_STATUS_UNKNOWN = psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN
TRANSACTION_STATUS_IDLE = psycopg2.extensions.TRANSACTION_STATUS_IDLE


class ConnInfo(Protocol):
    transaction_status: int


class Psycopg2Connection(Connection[Any], Protocol):
    autocommit: bool
    isolation_level: str
    info: ConnInfo

    def set_session(self, isolation_level: str) -> None: ...


class Psycopg2Transaction(Transaction, driver=psycopg2):
    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        conn.autocommit = True

    def _enable_params(self) -> None:
        conn = cast("Psycopg2Connection", self._current.conn)
        if conn.info.transaction_status != TRANSACTION_STATUS_IDLE:
            conn.rollback()
        conn.autocommit = False
        if level := self._params.get("level"):
            self._old_isolation_level = conn.isolation_level
            conn.set_session(isolation_level=level.upper())

    def _restore_params(self) -> None:
        conn = cast("Psycopg2Connection", self._current.conn)
        conn.autocommit = True
        if hasattr(self, "_old_isolation_level"):
            conn.set_session(isolation_level=self._old_isolation_level)

    def _start_savepoint(self) -> None:
        self._savepoint_name = f"sp_{id(self)}"
        conn = cast("Psycopg2Connection", self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()

    def _release_savepoint(self) -> None:
        conn = cast("Psycopg2Connection", self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()

    def _rollback_savepoint(self) -> None:
        conn = cast("Psycopg2Connection", self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()


class Psycopg2ConnectionValidator(ConnectionValidator, driver=psycopg2):
    def validate(self, conn: Any) -> bool:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except Exception:  # любая ошибка = соединение негодно
            return False
        return True

    def before_release(self, conn: Any) -> bool:
        if conn.closed:
            return False
        status = conn.info.transaction_status
        if status == TRANSACTION_STATUS_UNKNOWN:
            return False
        try:
            conn.rollback()
        except Exception:  # любая ошибка = соединение негодно
            return False
        return self.validate(conn)
