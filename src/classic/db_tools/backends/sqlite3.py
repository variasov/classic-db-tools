import sqlite3
from typing import Any, cast

from classic.db_tools.conn_validator import ConnectionValidator
from classic.db_tools.transaction import Transaction


class Sqlite3Transaction(Transaction, driver=sqlite3):
    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        conn.isolation_level = None

    def _enable_params(self) -> None:
        conn = cast("sqlite3.Connection", self._current.conn)
        conn.isolation_level = None
        conn.execute("BEGIN")

    def _restore_params(self) -> None:
        conn = cast("sqlite3.Connection", self._current.conn)
        conn.isolation_level = None

    def _start_savepoint(self) -> None:
        self._savepoint_name = f"savepoint_{id(self)}"
        conn = cast("sqlite3.Connection", self._current.conn)
        conn.execute(f"SAVEPOINT {self._savepoint_name}")

    def _release_savepoint(self) -> None:
        conn = cast("sqlite3.Connection", self._current.conn)
        conn.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")

    def _rollback_savepoint(self) -> None:
        conn = cast("sqlite3.Connection", self._current.conn)
        conn.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
        conn.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")


class Sqlite3ConnectionValidator(ConnectionValidator, driver=sqlite3):
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
        try:
            conn.rollback()
        except Exception:  # любая ошибка = соединение негодно
            return False
        return self.validate(conn)
