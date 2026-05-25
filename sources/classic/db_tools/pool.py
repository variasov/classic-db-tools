# Copyright 2020 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import ModuleType
from typing import Any, Callable, Optional
import threading
import queue
import logging

from . import exceptions
from .conn_validator import ConnectionValidator


logger = logging.getLogger(__name__)

ConnType = Any


class ConnectionPool:
    """
    A connection pool implementation using a queue to provide thread-safety.
    """

    # A callable returning a db-api connection object
    connection_factory: Callable[[], ConnType]

    # How many simultaneous connections to allow. If zero the number will be
    # unlimited
    limit: int

    # Callable to release a connection. Must return True if the connection
    # may be reused, else False.
    before_release: Optional[Callable[[ConnType], bool]]

    # Callable called every time a connection is acquired to validate
    # that it is still alive
    validate: Optional[Callable[[ConnType], bool]]

    # Maintain the pool in a queue for thread/process safety
    queue_class = queue.Queue
    lock_class = threading.Lock

    # How long to wait for a connection to become available
    timeout: float = 5.0

    _pool: queue.Queue

    def __init__(
        self,
        driver: ModuleType,
        connection_factory,
        timeout: float = 5.0,
        limit: int = 0,
        validator: ConnectionValidator = 'auto',
    ):
        self._pool = self.queue_class()
        self.driver = driver
        self.lock = self.lock_class()

        if isinstance(validator, ConnectionValidator):
            self.validate = validator.validate
            self.before_release = validator.before_release
        elif validator == "auto":
            validator = ConnectionValidator.validators[driver]()
            self.validate = validator.validate
            self.before_release = validator.before_release
        else:
            self.validate = None
            self.before_release = None

        self.connection_factory = connection_factory
        self.limit = limit
        self.max_validation_retries = self.limit + 3

        self.connections_created = 0
        self.reached_limit = False
        self.timeout = timeout

    def _getconn(self):
        """
        Return a connection from the pool.
        """
        try:
            return self._pool.get(
                block=self.reached_limit,
                timeout=self.timeout,
            )
        except queue.Empty:
            if self.limit:
                with self.lock:
                    if self.reached_limit:
                        raise exceptions.ConnectionLimitError()
                    else:
                        return self._connect()
            else:
                return self._connect()

    def getconn(self) -> ConnType:
        if not self.validate:
            return self._getconn()
        for retry in range(self.max_validation_retries):
            conn = self._getconn()
            if self.validate(conn):
                return conn
            self.release(conn)
        raise Exception(
            f"Could not validate a connection after "
            f"{self.max_validation_retries} attempts"
        )

    def connect(self) -> 'ContextManagerWrappedConnection':
        """
        Return a context manager that manages acquiring and releasing a
        connection.
        """
        return ContextManagerWrappedConnection(self)

    def _connect(self) -> ConnType:
        conn = self.connection_factory()  # type: ignore
        self.connections_created += 1
        self.reached_limit = (
            self.limit and self.connections_created >= self.limit
        )
        return conn

    def release(self, conn: ConnType) -> None:
        reuse = self.before_release(conn) if self.before_release else True
        if reuse:
            self._pool.put(conn)
        else:
            conn.close()
            if self.limit:
                self.lock.acquire()
                self.connections_created -= 1
                self.reached_limit = self.connections_created >= self.limit
                self.lock.release()


class ContextManagerWrappedConnection:
    conn: ConnType
    pool: ConnectionPool

    def __init__(self, pool: ConnectionPool):
        self.conn = None
        self.pool = pool

    def __enter__(self) -> ConnType:
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_value, tb) -> bool:
        self.pool.release(self.conn)
        return False
