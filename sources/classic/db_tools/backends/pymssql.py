import pymssql

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


class PyMSSQLTransaction(Transaction, driver=pymssql):
    _DEFAULT_ISOLATION = 'READ COMMITTED'

    def _enable_params(self):
        if level := self._params.get('level'):
            level_str = str(level).upper().replace('_', ' ')
            cursor = self._current.conn.cursor()
            try:
                cursor.execute(f'SET TRANSACTION ISOLATION LEVEL {level_str}')
            finally:
                cursor.close()

    def _restore_params(self):
        if self._params.get('level'):
            cursor = self._current.conn.cursor()
            try:
                cursor.execute(
                    f'SET TRANSACTION ISOLATION LEVEL {self._DEFAULT_ISOLATION}'
                )
            finally:
                cursor.close()

    def _start_savepoint(self):
        self._savepoint_name = f'sp_{id(self)}'
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'SAVE TRANSACTION {self._savepoint_name}')
        finally:
            cursor.close()

    def _release_savepoint(self):
        pass  # T-SQL has no RELEASE SAVEPOINT equivalent; changes are already part of outer TX

    def _rollback_savepoint(self):
        # ROLLBACK TRANSACTION <name> rolls back to savepoint only, not the entire outer TX
        cursor = self._current.conn.cursor()
        try:
            cursor.execute(f'ROLLBACK TRANSACTION {self._savepoint_name}')
        finally:
            cursor.close()


class PyMSSQLConnectionValidator(ConnectionValidator, driver=pymssql):

    def validate(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 AS [one]')
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
