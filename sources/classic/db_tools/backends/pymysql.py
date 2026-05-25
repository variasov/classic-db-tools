import pymysql

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class PyMySQLTransaction(Transaction, driver=pymysql):

    def _at_enter(self):
        self.current.conn.autocommit(False)

    def _at_exit(self):
        self.current.conn.autocommit(True)


class PyMySQLConnectionValidator(ConnectionValidator, driver=pymysql):

    def validate(self, conn):
        try:
            conn.ping()
        except Exception:
            return False
        return True
