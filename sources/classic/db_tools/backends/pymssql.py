import pymssql

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class PyMSSQLTransaction(Transaction, driver=pymssql):

    def _at_enter(self):
        self.current.conn.autocommit(False)

    def _at_exit(self):
        self.current.conn.autocommit(True)


class PyMSSQLConnectionValidator(ConnectionValidator, driver=pymssql):

    def validate(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 AS [1]")
            cursor.fetchone()
            cursor.close()
        except Exception:
            return False
        return True
