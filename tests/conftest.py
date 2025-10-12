import os.path
from typing import Any

from classic.db_tools import Engine, ConnectionPool
import pytest
import psycopg


SQL_DIR_PATH = os.path.join(os.path.dirname(__file__), 'sql')


def create_pool(
    factory_kwargs: dict[str, Any] = None,
    pool_kwargs: dict[str, Any] = None,
) -> ConnectionPool:
    env = os.environ
    return ConnectionPool(
        lambda: psycopg.connect(f'''
                host={env.get('DB_HOST', 'localhost')}
                port={env.get('DB_HOST', '5432')} 
                dbname={env.get('DB_NAME', 'tasks')} 
                user={env.get('DB_USER', 'test')} 
                password={env.get('DB_PASSWORD', 'test')} 
            ''', **factory_kwargs or {}),
        **pool_kwargs or {},
    )


@pytest.fixture
def conn_pool():
    yield create_pool(
        dict(autocommit=False), dict(limit=1),
    )


@pytest.fixture(scope='function')
def engine(conn_pool):
    with Engine(
        os.path.join(os.path.dirname(__file__), 'sql'),
        conn_pool,
        commit_on_exit=False,
    ) as engine:
        yield engine


@pytest.fixture
def ddl(engine: Engine):
    return engine.from_file('example/ddl.sql').execute()
