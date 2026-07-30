import os

import psycopg
import pytest

from classic.db_tools import Engine
from classic.db_tools.pool import ConnectionPool


def connect():
    return psycopg.connect(
        dbname=os.environ.get('PGDBNAME', 'test'),
        user=os.environ.get('PGUSER', 'test'),
        password=os.environ.get('PGPASSWORD', 'test'),
    )


@pytest.fixture(scope='session')
def engine():
    return Engine(
        psycopg, connect,
        templates_dirs=os.path.join(os.path.dirname(__file__), 'sql'),
        pool_class=ConnectionPool,
    )


@pytest.fixture
def standalone_engine():
    return Engine(psycopg, connect)


@pytest.fixture(scope='session', autouse=True)
def schema(engine):
    with engine.conn():
        engine.query(
            'CREATE TABLE IF NOT EXISTS items '
            '(id SERIAL PRIMARY KEY, title TEXT, value INT)',
            static=True,
        ).execute()
        engine.query(
            'CREATE TABLE IF NOT EXISTS _map_items '
            '(id SERIAL PRIMARY KEY, title TEXT, value INT)',
            static=True,
        ).execute()
        engine.query(
            'CREATE TABLE IF NOT EXISTS _map_parents '
            '(id INT PRIMARY KEY, name TEXT)',
            static=True,
        ).execute()
        engine.query(
            'CREATE TABLE IF NOT EXISTS _map_children '
            '(id SERIAL PRIMARY KEY, parent_id INT, label TEXT)',
            static=True,
        ).execute()
        engine.query(
            'CREATE TABLE IF NOT EXISTS _map_tags '
            '(id SERIAL PRIMARY KEY, parent_id INT, name TEXT)',
            static=True,
        ).execute()


@pytest.fixture(autouse=True)
def data(engine, schema):
    with engine.transaction(commit=False):
        yield
