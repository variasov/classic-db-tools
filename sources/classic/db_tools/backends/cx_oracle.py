import cx_Oracle

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class CxOracleTransaction(Transaction, driver=cx_Oracle):

    def _enable_params(self):
        conn = self._current.conn
        conn.autocommit = False
        if level := self._params.get('level'):
            level = level.upper()
            # Oracle supports only READ COMMITTED and SERIALIZABLE
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f'SET TRANSACTION ISOLATION LEVEL {level}'
                )
            finally:
                cursor.close()

    def _restore_params(self):
        pass

    def _start_savepoint(self):
        self._savepoint_name = f'sp_{id(self)}'
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()

    def _release_savepoint(self):
        # Oracle does not support RELEASE SAVEPOINT — savepoint is implicitly
        # released when the outer transaction commits or the connection closes
        pass

    def _rollback_savepoint(self):
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'ROLLBACK TO SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()


class CxOracleConnectionValidator(ConnectionValidator, driver=cx_Oracle):

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
