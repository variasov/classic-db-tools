import psycopg

from ..transaction import Transaction
from ..conn_validator import ConnectionValidator


UNKNOWN = psycopg.pq.TransactionStatus.UNKNOWN
IDLE = psycopg.pq.TransactionStatus.IDLE


class PsycopgTransaction(Transaction, driver=psycopg):

    def _enable_params(self):
        conn: psycopg.Connection = self._current.conn

        if conn.info.transaction_status != IDLE:
            conn.rollback()

        conn.autocommit = False

        if readonly := self._params.get('readonly'):
            self.old_read_only = conn.read_only
            conn.read_only = readonly

        if level := self._params.get('level'):
            self.old_isolation_level = conn.isolation_level
            conn.isolation_level = level

        if deferrable := self._params.get('deferrable'):
            self.old_deferrable = conn.deferrable
            conn.deferrable = deferrable

    def _restore_params(self):
        conn: psycopg.Connection = self._current.conn
        conn.autocommit = True

        old_read_only = getattr(self, 'old_read_only', None)
        if old_read_only is not None:
            conn.read_only = old_read_only

        old_isolation_level = getattr(self, 'old_isolation_level', None)
        if old_isolation_level is not None:
            conn.isolation_level = old_isolation_level

        old_deferrable = getattr(self, 'old_deferrable', None)
        if old_deferrable is not None:
            conn.deferrable = old_deferrable

    def _start_savepoint(self):
        pass

    def _release_save_point(self):
        pass


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
