import oracledb

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class OracleDBTransaction(Transaction, driver=oracledb):

    def _at_enter(self):
        self.current.conn.autocommit = False

    def _at_exit(self):
        self.current.conn.autocommit = True


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
