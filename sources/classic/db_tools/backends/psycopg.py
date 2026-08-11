from typing import TypedDict, cast

import psycopg
from psycopg.sql import SQL

from ..conn_validator import ConnectionValidator
from ..transaction import Transaction


UNKNOWN = psycopg.pq.TransactionStatus.UNKNOWN
IDLE = psycopg.pq.TransactionStatus.IDLE


class PsycopgTxParams(TypedDict):
    readonly: bool
    level: str
    deferrable: bool


class PsycopgTransaction(Transaction, driver=psycopg):

    @classmethod
    def enable_autocommit(cls, conn):
        conn.autocommit = True

    def _enable_params(self):
        conn = cast(psycopg.Connection, self._current.conn)

        if conn.info.transaction_status != IDLE:
            conn.rollback()

        conn.autocommit = False

        params = cast(PsycopgTxParams, self._params)
        if readonly := params.get('readonly'):
            self.old_read_only = conn.read_only
            conn.read_only = readonly

        if level := params.get('level'):
            self.old_isolation_level = conn.isolation_level
            if isinstance(level, str):
                level = psycopg.IsolationLevel[level.upper()]
            conn.isolation_level = level

        if deferrable := params.get('deferrable'):
            self.old_deferrable = conn.deferrable
            conn.deferrable = deferrable

    def _restore_params(self):
        conn = cast(psycopg.Connection, self._current.conn)

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
        self._savepoint_name = f'savepoint_{id(self)}'
        conn = cast(psycopg.Connection, self._current.conn)
        conn.execute(
            SQL('SAVEPOINT {}').format(self._savepoint_name),
        )

    def _release_savepoint(self):
        conn = cast(psycopg.Connection, self._current.conn)
        conn.execute(
            SQL('RELEASE SAVEPOINT {}').format(self._savepoint_name),
        )

    def _rollback_savepoint(self):
        conn = cast(psycopg.Connection, self._current.conn)
        conn.execute(
            SQL('ROLLBACK TO {}').format(self._savepoint_name),
        )
        conn.execute(
            SQL('RELEASE SAVEPOINT {}').format(self._savepoint_name),
        )


class PsycopgConnectionValidator(ConnectionValidator, driver=psycopg):

    def validate(self, conn):
        try:
            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.fetchone()
            cur.close()
        except Exception:
            return False
        return True

    def before_release(self, conn: psycopg.Connection):
        if conn.closed:
            return False
        status = conn.info.transaction_status
        if status == UNKNOWN:
            return False
        try:
            conn.rollback()
        except Exception:
            return False
        return self.validate(conn)
