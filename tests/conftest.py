import os.path

from classic.db_tools import Engine
import pytest
import psycopg


SQL_DIR_PATH = os.path.join(os.path.dirname(__file__), 'sql')


@pytest.fixture(scope='session')
def engine():
    env = os.environ
    return Engine(
        psycopg,
        lambda: psycopg.connect(f'''
            host={env.get('DB_HOST', 'localhost')}
            port={env.get('DB_HOST', '5432')} 
            dbname={env.get('DB_NAME', 'tasks')} 
            user={env.get('DB_USER', 'test')} 
            password={env.get('DB_PASSWORD', 'test')} 
        '''),
        templates_dirs=os.path.join(os.path.dirname(__file__), 'sql'),
        pool_kwargs=dict(limit=1),
    )


@pytest.fixture(scope='function')
def tx(engine: Engine):
    with engine.transaction(commit=False):
        yield engine


@pytest.fixture(scope='function')
def ddl(engine: Engine, tx):
    yield engine.query_from('example/ddl.sql').execute()
