from classic.db_tools import Engine

import pytest

from .conftest import SQL_DIR_PATH


def test_engine_works_with_outer_cursor(conn_pool) -> None:
    engine = Engine(SQL_DIR_PATH, conn_pool, commit_on_exit=False)

    with conn_pool.connect() as conn:
        assert engine.query('SELECT 1').scalar(_cursor=conn.cursor()) == 1


def test_engine_fail_without_enter_cx(conn_pool):
    engine = Engine(SQL_DIR_PATH, conn_pool, commit_on_exit=False)

    with pytest.raises(AttributeError):
        assert engine.query('SELECT 1').scalar() == 1
