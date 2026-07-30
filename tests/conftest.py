import os.path

from classic.db_tools import Engine
import pytest
import psycopg


SQL_DIR_PATH = os.path.join(os.path.dirname(__file__), 'sql')


@pytest.fixture(scope='session')
def engine():
    return Engine(
        psycopg,
        templates_dirs=os.path.join(os.path.dirname(__file__), 'sql'),
    )


@pytest.fixture(scope='function')
def tx(engine: Engine):
    with engine.transaction(commit=False):
        yield engine


@pytest.fixture(scope='function')
def ddl(engine: Engine, tx):
    _ = tx  # for linting
    yield engine.query_from('example/ddl.sql').execute()
