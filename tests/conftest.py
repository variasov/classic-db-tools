import os.path

from classic.db_tools import Engine, ConnectionPool
import pytest
import psycopg


@pytest.fixture
def conn_pool():
    env = os.environ
    yield ConnectionPool(
        lambda: psycopg.connect(f'''
            host={env.get('DB_HOST', 'localhost')}
            port={env.get('DB_HOST', '5432')} 
            dbname={env.get('DB_NAME', 'tasks')} 
            user={env.get('DB_USER', 'test')} 
            password={env.get('DB_PASSWORD', 'test')} 
        '''),
        limit=1,
    )


@pytest.fixture(scope='function')
def engine(conn_pool):
    with Engine(
        os.path.join(os.path.dirname(__file__), 'sql'),
        conn_pool,
    ) as engine:
        yield engine
        engine.rollback()


@pytest.fixture
def ddl(engine: Engine):
    return engine.from_file('example/ddl.sql').execute()
