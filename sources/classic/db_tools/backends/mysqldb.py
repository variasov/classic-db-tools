import MySQLdb

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class MySQLDBTransaction(Transaction, driver=MySQLdb):

    def _at_enter(self):
        self.current.conn.autocommit(False)

    def _at_exit(self):
        self.current.conn.autocommit(True)


class MySQLDBConnectionValidator(ConnectionValidator, driver=MySQLdb):

    def validate(self, conn):
        try:
            conn.ping()
        except Exception:
            return False
        return True
