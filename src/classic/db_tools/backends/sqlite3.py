import sqlite3
from typing import cast

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class Sqlite3Transaction(Transaction, driver=sqlite3):

    @classmethod
    def enable_autocommit(cls, conn):
        conn.isolation_level = None

    def _enable_params(self):
        conn = cast(sqlite3.Connection, self._current.conn)
        conn.isolation_level = None
        conn.execute('BEGIN')

    def _restore_params(self):
        conn = cast(sqlite3.Connection, self._current.conn)
        conn.isolation_level = None

    def _start_savepoint(self):
        self._savepoint_name = f'savepoint_{id(self)}'
        conn = cast(sqlite3.Connection, self._current.conn)
        conn.execute(f'SAVEPOINT {self._savepoint_name}')

    def _release_savepoint(self):
        conn = cast(sqlite3.Connection, self._current.conn)
        conn.execute(f'RELEASE SAVEPOINT {self._savepoint_name}')

    def _rollback_savepoint(self):
        conn = cast(sqlite3.Connection, self._current.conn)
        conn.execute(f'ROLLBACK TO SAVEPOINT {self._savepoint_name}')
        conn.execute(f'RELEASE SAVEPOINT {self._savepoint_name}')


class Sqlite3ConnectionValidator(ConnectionValidator, driver=sqlite3):

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
        try:
            conn.rollback()
        except Exception:
            return False
        return self.validate(conn)
