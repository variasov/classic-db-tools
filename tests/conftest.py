import os.path

from classic.sql_tools.module import Module
import pytest
import psycopg


@pytest.fixture(scope='session')
def queries():
    return Module(os.path.join(os.path.dirname(__file__), 'sql'))


@pytest.fixture(scope='session')
def psycopg_conn():
    env = os.environ
    conn = psycopg.connect(
        conninfo=f'''
            host={env.get('DB_HOST', 'localhost')}
            port={env.get('DB_HOST', '5432')} 
            dbname={env.get('DB_NAME', 'tests')} 
            user={env.get('DB_USER', 'test')} 
            password={env.get('DB_PASSWORD', 'test')} 
        ''',
    )
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture(scope='function')
def connection(psycopg_conn):
    with psycopg_conn.transaction(
        savepoint_name='pre_test',
        force_rollback=True
    ) as conn:
        yield psycopg_conn
    psycopg_conn.rollback()
