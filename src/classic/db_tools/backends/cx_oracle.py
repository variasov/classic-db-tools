import sys
import warnings
from typing import Any

import cx_Oracle

from classic.db_tools.conn_validator import ConnectionValidator
from classic.db_tools.transaction import Transaction

if sys.version_info >= (3, 11):
    warnings.warn(
        "The cx_Oracle backend is deprecated: the cx_Oracle package "
        "cannot be built for Python 3.11+, use oracledb instead",
        DeprecationWarning,
        stacklevel=2,
    )


class CxOracleTransaction(Transaction, driver=cx_Oracle):
    @classmethod
    def enable_autocommit(cls, conn: Any) -> None:
        conn.autocommit = True

    def _enable_params(self) -> None:
        conn: Any = self._current.conn
        conn.autocommit = False
        if level := self._params.get("level"):
            # Oracle supports only READ COMMITTED and SERIALIZABLE
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
            finally:
                cursor.close()

    def _restore_params(self) -> None:
        conn: Any = self._current.conn
        conn.autocommit = True

    def _start_savepoint(self) -> None:
        self._savepoint_name = f"sp_{id(self)}"
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()

    def _release_savepoint(self) -> None:
        # Oracle does not support RELEASE SAVEPOINT — savepoint is implicitly
        # released when the outer transaction commits or the connection closes
        pass

    def _rollback_savepoint(self) -> None:
        conn: Any = self._current.conn
        cursor = conn.cursor()
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
        finally:
            cursor.close()


class CxOracleConnectionValidator(ConnectionValidator, driver=cx_Oracle):
    def validate(self, conn: Any) -> bool:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
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
