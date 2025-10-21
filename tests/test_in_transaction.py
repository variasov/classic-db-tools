import pytest

from classic.components import component
from classic.db_tools import Engine, in_transaction
import psycopg


@component
class SomeCls:
    db: Engine

    @in_transaction
    def run(self):
        self.db.query("INSERT INTO tasks VALUES ('1', '2')")
        raise ValueError


def test__in_transaction(engine: Engine, ddl):
    with pytest.raises(ValueError):
        SomeCls(db=engine).run()

    with pytest.raises(psycopg.errors.UndefinedTable):
        engine.query('SELECT * FROM tasks').execute()
