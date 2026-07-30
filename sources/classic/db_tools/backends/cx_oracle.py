import cx_Oracle

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class CxOracleTransaction(Transaction, driver=cx_Oracle):

    def _at_enter(self):
        self._current.conn.autocommit = False

    def _at_exit(self):
        self._current.conn.autocommit = True


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
