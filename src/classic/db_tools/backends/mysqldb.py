from typing import Any

import MySQLdb

from classic.db_tools.conn_validator import ConnectionValidator
from classic.db_tools.transaction import Transaction


class MySQLDBTransaction(Transaction, driver=MySQLdb):
    _DEFAULT_ISOLATION = "REPEATABLE READ"

    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        conn.autocommit(True)

    def _enable_params(self) -> None:
        conn: Any = self._current.conn
        conn.autocommit(False)
        if level := self._params.get("level"):
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SET SESSION TRANSACTION ISOLATION LEVEL {level}"
                )
            finally:
                cursor.close()

    def _restore_params(self) -> None:
        conn: Any = self._current.conn
        if self._params.get("level"):
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SET SESSION TRANSACTION ISOLATION LEVEL "
                    f"{self._DEFAULT_ISOLATION}"
                )
            finally:
                cursor.close()
        conn.autocommit(True)

    def _start_savepoint(self) -> None:
        self._savepoint_name = f"sp_{id(self)}"
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()

    def _release_savepoint(self) -> None:
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()

    def _rollback_savepoint(self) -> None:
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()


class MySQLDBConnectionValidator(ConnectionValidator, driver=MySQLdb):
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
