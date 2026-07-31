import psycopg2

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction

TRANSACTION_STATUS_UNKNOWN = psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN
TRANSACTION_STATUS_IDLE = psycopg2.extensions.TRANSACTION_STATUS_IDLE


class Psycopg2Transaction(Transaction, driver=psycopg2):

    def _enable_params(self):
        conn = self._current.conn
        if conn.info.transaction_status != TRANSACTION_STATUS_IDLE:
            conn.rollback()
        conn.autocommit = False
        if level := self._params.get('level'):
            self._old_isolation_level = conn.isolation_level
            conn.set_session(isolation_level=level.upper())

    def _restore_params(self):
        conn = self._current.conn
        if hasattr(self, '_old_isolation_level'):
            conn.set_session(isolation_level=self._old_isolation_level)

    def _start_savepoint(self):
        self._savepoint_name = f'sp_{id(self)}'
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()

    def _release_savepoint(self):
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'RELEASE SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()

    def _rollback_savepoint(self):
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'ROLLBACK TO SAVEPOINT {self._savepoint_name}')
            cursor.execute(f'RELEASE SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()


class Psycopg2ConnectionValidator(ConnectionValidator, driver=psycopg2):

    def validate(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            cursor.fetchone()
            cursor.close()
        except Exception:
            return False
        return True

    def before_release(self, conn):
        if conn.closed:
            return False
        status = conn.info.transaction_status
        if status == TRANSACTION_STATUS_UNKNOWN:
            return False
        try:
            conn.rollback()
        except Exception:
            return False
        return self.validate(conn)
