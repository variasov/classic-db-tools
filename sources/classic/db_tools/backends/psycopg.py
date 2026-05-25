import psycopg

from ..transaction import Transaction
from ..conn_validator import ConnectionValidator


UNKNOWN = psycopg.pq.TransactionStatus.UNKNOWN
IDLE = psycopg.pq.TransactionStatus.IDLE


class PsycopgTransaction(Transaction, driver=psycopg):

    def _at_enter(self):
        conn: psycopg.Connection = self.current.conn

        if conn.info.transaction_status != IDLE:
            conn.rollback()

        conn.autocommit = False

        if readonly := self.params.get('readonly'):
            self.old_read_only = conn.read_only
            conn.read_only = readonly

        if level := self.params.get('level'):
            self.old_isolation_level = conn.isolation_level
            conn.isolation_level = level

    def _at_exit(self):
        conn: psycopg.Connection = self.current.conn
        conn.autocommit = True

        old_read_only = getattr(self, 'old_read_only', None)
        if old_read_only is not None:
            conn.read_only = old_read_only

        old_isolation_level = getattr(self, 'old_isolation_level', None)
        if old_isolation_level is not None:
            conn.isolation_level = old_isolation_level


class PsycopgConnectionValidator(ConnectionValidator, driver=psycopg):

    def validate(self, conn):
        try:
            cur = conn.cursor()
            cur.execute('SELECT 1')
        except Exception:
            return False
        return True

    def before_release(self, conn: psycopg.Connection):
        if conn.closed:
            return False
        status = conn.info.transaction_status
        if status == UNKNOWN:
            return False
        elif status != IDLE:
            conn.rollback()
            return True
        return True
