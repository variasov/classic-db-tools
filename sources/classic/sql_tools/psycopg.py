import functools
import threading
from typing import TypeVar

from classic.components import doublewrap, component

from psycopg import Connection, Transaction
from psycopg_pool import ConnectionPool

from .module import Module


T = TypeVar('T')


@doublewrap
def transactional(
    fn: T,
    savepoint_name: str | None = None,
    force_rollback: bool = False,
) -> T:

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self.conn.transaction(savepoint_name, force_rollback):
            return fn(*args, **kwargs)

    return wrapper


@doublewrap
def pipeline(fn):

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self.conn.pipeline():
            return fn(*args, **kwargs)

    return wrapper


class ConnectionContext(threading.local):
    conn_pool: ConnectionPool
    conn: Connection

    def __init__(self, conn_pool: ConnectionPool):
        self.conn_pool = conn_pool

    def __enter__(self) -> Connection:
        self.cm = self.conn_pool.connection()
        self.conn = self.cm.__enter__()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.conn
        return self.cm.__exit__(exc_type, exc_val, exc_tb)


class TransactionContext(ConnectionContext):
    tx: Transaction

    def __enter__(self):
        conn = super().__enter__()
        self.tx = conn.transaction().__enter__()
        return self.tx

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tx.__exit__(exc_type, exc_val, exc_tb)
        return super().__exit__(exc_type, exc_val, exc_tb)


@component
class DBInterface:
    queries: Module
    ctx: ConnectionContext

    @property
    def conn(self) -> Connection:
        return self.ctx.conn
