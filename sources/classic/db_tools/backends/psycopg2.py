import psycopg2

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


TRANSACTION_STATUS_UNKNOWN = psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN
TRANSACTION_STATUS_IDLE = psycopg2.extensions.TRANSACTION_STATUS_IDLE


class Psycopg2Transaction(Transaction, driver=psycopg2):

    def _at_enter(self):
        self._current.conn.rollback()
        self._current.conn.autocommit = False

    def _at_exit(self):
        self._current.conn.autocommit = True


class Psycopg2ConnectionValidator(ConnectionValidator, driver=psycopg2):

    def validate(self, conn):
        try:
            cur = conn.cursor()
            cur.execute('SELECT 1')
        except Exception:
            return False
        return True

    def before_release(self, conn):
        if conn.closed:
            return False
        status = conn.info.transaction_status
        if status == TRANSACTION_STATUS_UNKNOWN:
            return False
        elif status != TRANSACTION_STATUS_IDLE:
            conn.rollback()
            return True
        return True
