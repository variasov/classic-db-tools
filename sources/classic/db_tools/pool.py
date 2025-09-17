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

from typing import Any
from typing import Callable
from typing import Optional
import threading
import queue
import logging

from . import exceptions
from . import poolvalidators

logger = logging.getLogger(__name__)

ConnType = Any


class ConnectionPoolBase:
    """
    Connection pool base class
    """

    #: A callable returning a db-api connection object
    connection_factory: Callable[[], ConnType]

    #: How many simultaneous connections to allow. If zero the number will be
    #: unlimited
    limit: int

    #: Callable to release a connection. Must return True if the connection
    #: may be reused, else False.
    before_release: Optional[Callable[[ConnType], bool]]

    #: Callable called every time a connection is acquired to validate
    #: that it is still alive
    validate: Optional[Callable[[ConnType], bool]]

    def __init__(self, connection_factory, limit=0, validator="auto"):
        if isinstance(validator, poolvalidators.ConnectionValidator):
            self.validate = validator.validate
            self.before_release = validator.before_release
        elif validator == "auto":
            self.validate = self.auto_validate  # type: ignore
            self.before_release = None
        else:
            self.validate = None
            self.before_release = None
        self.connection_factory = connection_factory  # type: ignore # noqa
        self.limit = limit
        self.max_validation_retries = self.limit + 3

    def _getconn(self) -> ConnType:
        raise NotImplementedError()

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

    def release(self, conn: ConnType):
        raise NotImplementedError()

    def connect(self):
        """
        Return a context manager that manages acquiring and releasing a
        connection.
        """
        return ContextManagerWrappedConnection(self)

    def set_validator(self, v):
        self.validate = v.validate
        self.before_release = v.before_release

    def auto_validate(self, conn):
        validator = poolvalidators.ConnectionValidator()

        for cls in poolvalidators.validators:
            if isinstance(conn, cls):
                validator = poolvalidators.validators[cls]()
                break
        self.set_validator(validator)
        return validator.validate(conn)


class ConnectionPool(ConnectionPoolBase):
    """
    A connection pool implementation using a queue to provide thread-safety.
    """

    # Maintain the pool in a queue for thread/process safety
    queue_class = queue.Queue
    lock_class = threading.Lock

    #: How long to wait for a connection to become available
    timeout = 5

    _pool: queue.Queue

    def __init__(self, connection_factory, limit=0, validator="auto", timeout=timeout):
        super(ConnectionPool, self).__init__(connection_factory, limit, validator)

        self._pool = self.queue_class()
        self.lock = self.lock_class()
        self.connections_created = 0
        self.reached_limit = False
        self.timeout = timeout

    def _getconn(self):
        """
        Return a connection from the pool.
        """
        try:
            return self._pool.get(block=self.reached_limit, timeout=self.timeout)
        except queue.Empty:
            if self.limit:
                with self.lock:
                    if self.reached_limit:
                        raise exceptions.ConnectionLimitError()
                    else:
                        return self._connect()
            else:
                return self._connect()

    def _connect(self):
        conn = self.connection_factory()  # type: ignore
        self.connections_created += 1
        self.reached_limit = bool(self.limit and self.connections_created >= self.limit)
        return conn

    def release(self, conn):
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


class SQLAlchemyPool(ConnectionPoolBase):
    def __init__(self, connection_factory, limit=0, validator=None):
        if limit:
            raise ValueError(
                "Cannot set limit here, "
                "configure this in SQLAlchemy connection pooling instead"
            )
        if validator:
            raise ValueError(
                "Cannot set validator here, "
                "configure this in SQLAlchemy connection pooling instead"
            )

        def _connection_factory(sessiongetter=connection_factory):
            return sessiongetter().connection().connection.connection

        super(SQLAlchemyPool, self).__init__(_connection_factory, limit, validator)

    def getconn(self):
        return self.connection_factory()  # type: ignore

    def release(self, conn):
        pass


class ContextManagerWrappedConnection:
    def __init__(self, pool):
        self.conn = None
        self.pool = pool

    def __enter__(self):
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_value, tb):
        self.pool.release(self.conn)
