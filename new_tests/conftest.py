import os
import pytest
from classic.db_tools import Engine
import psycopg


@pytest.fixture
def engine():
    factory = lambda: psycopg.connect(
        dbname=os.environ['PGDBNAME'],
        user=os.environ['PGUSER'],
        password=os.environ['PGPASSWORD'],
        host=os.environ['PGHOST'],
        port=int(os.environ['PGPORT']),
    )

    return Engine(
        psycopg,
        factory,
        templates_dirs=os.path.join(os.path.dirname(__file__), 'sql'),
    )


@pytest.fixture
def tx(engine):
    with engine.transaction(commit=False):
        yield engine