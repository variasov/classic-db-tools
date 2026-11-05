from typing import cast

import pymysql

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class PyMySQLTransaction(Transaction, driver=pymysql):
    _DEFAULT_ISOLATION = 'REPEATABLE READ'

    @classmethod
    def enable_autocommit(cls, conn):
        pass

    def _enable_params(self):
        if level := self._params.get('level'):
            level_str = str(level).upper().replace('_', ' ')
            conn = cast(pymysql.Connection, self._current.conn)
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f'SET SESSION TRANSACTION ISOLATION LEVEL {level_str}'
                )
            finally:
                cursor.close()

    def _restore_params(self):
        if self._params.get('level'):
            conn = cast(pymysql.Connection, self._current.conn)
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f'SET SESSION TRANSACTION ISOLATION LEVEL '
                    f'{self._DEFAULT_ISOLATION}'
                )
            finally:
                cursor.close()

    def _start_savepoint(self):
        self._savepoint_name = f'sp_{id(self)}'
        conn = cast(pymysql.Connection, self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f'SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()

    def _release_savepoint(self):
        conn = cast(pymysql.Connection, self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f'RELEASE SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()

    def _rollback_savepoint(self):
        conn = cast(pymysql.Connection, self._current.conn)
        cursor = conn.cursor()
        try:
            cursor.execute(f'ROLLBACK TO SAVEPOINT {self._savepoint_name}')
            cursor.execute(f'RELEASE SAVEPOINT {self._savepoint_name}')
        finally:
            cursor.close()


class PyMySQLConnectionValidator(ConnectionValidator, driver=pymysql):

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
