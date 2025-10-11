from classic.db_tools import ConnectionPool

import psycopg


def test_connection_pool(conn_pool: ConnectionPool):
    with conn_pool.connect() as connection:
        assert isinstance(connection, psycopg.Connection)
