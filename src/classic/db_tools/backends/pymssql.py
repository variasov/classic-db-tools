from typing import Any

import pymssql

from classic.db_tools.conn_validator import ConnectionValidator
from classic.db_tools.transaction import Transaction


class PyMSSQLTransaction(Transaction, driver=pymssql):
    _DEFAULT_ISOLATION = "READ COMMITTED"

    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        conn.autocommit(True)

    def _enable_params(self) -> None:
        conn: Any = self._current.conn
        conn.autocommit(False)
        if level := self._params.get("level"):
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
            finally:
                cursor.close()

    def _restore_params(self) -> None:
        conn: Any = self._current.conn
        if self._params.get("level"):
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SET TRANSACTION ISOLATION LEVEL {self._DEFAULT_ISOLATION}"
                )
            finally:
                cursor.close()
        conn.autocommit(True)

    def _start_savepoint(self) -> None:
        self._savepoint_name = f"sp_{id(self)}"
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"SAVE TRANSACTION {self._savepoint_name}")
        finally:
            cursor.close()

    def _release_savepoint(self) -> None:
        # T-SQL has no RELEASE SAVEPOINT equivalent;
        # changes are already part of outer TX
        pass

    def _rollback_savepoint(self) -> None:
        # ROLLBACK TRANSACTION <name> rolls back to savepoint only,
        # not the entire outer TX
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"ROLLBACK TRANSACTION {self._savepoint_name}")
        finally:
            cursor.close()


class PyMSSQLConnectionValidator(ConnectionValidator, driver=pymssql):
    def validate(self, conn: Any) -> bool:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 AS [one]")
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
