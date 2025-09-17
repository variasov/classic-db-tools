import os.path

from classic.db_tools import Engine
import pytest
import psycopg


@pytest.fixture(scope='function')
def engine():
    env = os.environ
    with Engine(
        lambda: psycopg.connect(f'''
            host={env.get('DB_HOST', 'localhost')}
            port={env.get('DB_HOST', '5432')} 
            dbname={env.get('DB_NAME', 'tasks')} 
            user={env.get('DB_USER', 'test')} 
            password={env.get('DB_PASSWORD', 'test')} 
        '''),
        os.path.join(os.path.dirname(__file__), 'sql'),
    ) as engine:
        yield engine
        engine.rollback()


@pytest.fixture
def ddl(engine: Engine):
    return engine.queries.example.ddl()
