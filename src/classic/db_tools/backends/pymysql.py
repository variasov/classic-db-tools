from typing import Any

import pymysql

from classic.db_tools.conn_validator import ConnectionValidator
from classic.db_tools.transaction import Transaction


class PyMySQLTransaction(Transaction, driver=pymysql):
    _DEFAULT_ISOLATION = "REPEATABLE READ"

    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        pass

    def _enable_params(self) -> None:
        if level := self._params.get("level"):
            level_str = str(level).upper().replace("_", " ")
            conn: Any = self._current.conn
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SET SESSION TRANSACTION ISOLATION LEVEL {level_str}"
                )
            finally:
                cursor.close()

    def _restore_params(self) -> None:
        if self._params.get("level"):
            conn: Any = self._current.conn
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SET SESSION TRANSACTION ISOLATION LEVEL "
                    f"{self._DEFAULT_ISOLATION}"
                )
            finally:
                cursor.close()

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


class PyMySQLConnectionValidator(ConnectionValidator, driver=pymysql):
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
