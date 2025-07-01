import os.path

from classic.sql_tools import Engine
import pytest
import psycopg


@pytest.fixture(scope='session')
def engine():
    return Engine(
        os.path.join(os.path.dirname(__file__), 'sql')
    )


@pytest.fixture(scope='session')
def psycopg_conn():
    env = os.environ
    conn = psycopg.connect(f'''
        host={env.get('DB_HOST', 'localhost')}
        port={env.get('DB_HOST', '5432')} 
        dbname={env.get('DB_NAME', 'tasks')} 
        user={env.get('DB_USER', 'test')} 
        password={env.get('DB_PASSWORD', 'test')} 
    ''')
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture(scope='function')
def connection(psycopg_conn):
    with psycopg_conn.transaction(
        savepoint_name='pre_test',
        force_rollback=True,
    ):
        yield psycopg_conn
    psycopg_conn.rollback()


@pytest.fixture
def ddl(engine, connection):
    engine.example.ddl(connection)
    yield


@pytest.fixture
def tasks(engine, connection, ddl):
    engine.example.save_task(connection, [
        {'id': 1, 'name': 'First'},
        {'id': 2, 'name': 'Second'},
        {'id': 3, 'name': 'Third'},
    ])
    engine.example.save_task_statuses(connection, [
        {'status': 'CREATED', 'task_id': 1, 'id': 1},
        {'status': 'STARTED', 'task_id': 1, 'id': 4},
        {'status': 'FINISHED', 'task_id': 1, 'id': 5},
        {'status': 'CREATED', 'task_id': 2, 'id': 2},
        {'status': 'CREATED', 'task_id': 3, 'id': 3},
    ])
    yield
