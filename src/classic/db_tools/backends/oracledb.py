from typing import cast

import oracledb

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class OracleDBTransaction(Transaction, driver=oracledb):

    @classmethod
    def enable_autocommit(cls, conn):
        conn.autocommit = True

    def _enable_params(self):
        conn = cast(oracledb.Connection, self._current.conn)
        conn.autocommit = False
        if level := self._params.get('level'):
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f'SET TRANSACTION ISOLATION LEVEL {level}'
                )
            finally:
                cursor.close()

    def _restore_params(self):
        conn = cast(oracledb.Connection, self._current.conn)
        conn.autocommit = True

    def _start_savepoint(self):
        self._savepoint_name = f'sp_{id(self)}'
        conn = cast(oracledb.Connection, self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f'SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()

    def _release_savepoint(self):
        # Oracle does not support RELEASE SAVEPOINT — implicitly released
        # when the outer transaction commits or the connection closes
        pass

    def _rollback_savepoint(self):
        conn = cast(oracledb.Connection, self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f'ROLLBACK TO SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()


class OracleDBConnectionValidator(ConnectionValidator, driver=oracledb):

    def validate(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
        except Exception:
            return False
        return True

    def before_release(self, conn):
        try:
            conn.rollback()
        except Exception:
            return False
        return self.validate(conn)
