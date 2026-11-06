from typing import Any

from classic.db_tools import Engine


def test_autocommit(engine: Engine[Any]):
    with engine.transaction(commit=True):
        engine.query("CREATE TABLE autocommit_test(id int)").execute()
        engine.query("INSERT INTO autocommit_test(id) VALUES (10)").execute()


def test_autocommit_select(engine: Engine[Any]):
    assert engine.query("SELECT * FROM autocommit_test").all() == [(10,)]
