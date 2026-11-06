import copy
import gc
import sqlite3
from dataclasses import dataclass

import pytest

from classic.db_tools import (
    CursorOutsideScopeError,
    DetachedCursor,
    Engine,
    Entity,
)
from classic.db_tools.pool import ConnectionLimitError


@dataclass
class Item:
    id: int = 0
    title: str = ""


SELECT = "SELECT id, title FROM t ORDER BY id"
SELECT_MAPPED = "SELECT id AS item__id, title AS item__title FROM t ORDER BY id"


@pytest.fixture
def eng(tmp_path):
    path = str(tmp_path / "db.sqlite")
    engine = Engine(
        sqlite3,
        lambda: sqlite3.connect(path),
        pool_kwargs={"limit": 2, "timeout": 0.2},
    )
    with engine.conn():
        engine.query(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, title TEXT)",
            static=True,
        ).execute()
        for i in range(1, 6):
            engine.query(
                "INSERT INTO t (id, title) VALUES (?, ?)",
                static=True,
            ).execute((i, f"n{i}"))
    return engine


def idle(engine):
    return engine._pool._pool.qsize()


def created(engine):
    return engine._pool.connections_created


def test_exports():
    assert issubclass(CursorOutsideScopeError, RuntimeError)
    assert DetachedCursor


def test_sql_error_raised_on_call_and_conn_released(eng):
    before = idle(eng)
    with pytest.raises(sqlite3.OperationalError):
        eng.query("SELECT * FROM missing", static=True).iter()
    with pytest.raises(sqlite3.OperationalError):
        eng.query("SELECT * FROM missing", static=True).scalars()
    assert idle(eng) == before


def test_iter_outside_scope_holds_own_conn_until_exhausted(eng):
    q = eng.query(SELECT, static=True)
    before = idle(eng)
    rows = q.iter(batch=2)
    assert idle(eng) == before - 1
    assert eng.current.conn is None
    assert next(rows) == (1, "n1")
    assert idle(eng) == before - 1
    assert [r[0] for r in rows] == [2, 3, 4, 5]
    assert idle(eng) == before


def test_iter_conn_is_not_in_scope(eng):
    rows = eng.query(SELECT, static=True).iter()
    with eng.conn():
        assert eng.current.conn is not rows._cursor.connection
    assert next(rows) == (1, "n1")
    rows.close()


def test_scalars_outside_scope_released_after_exhaustion(eng):
    before = idle(eng)
    rows = eng.query(SELECT, static=True).scalars(batch=2)
    assert idle(eng) == before - 1
    assert list(rows) == [1, 2, 3, 4, 5]
    assert idle(eng) == before


def test_limit_one_connection_reusable_after_exhaustion(tmp_path):
    path = str(tmp_path / "one.sqlite")
    engine = Engine(
        sqlite3,
        lambda: sqlite3.connect(path),
        pool_kwargs={"limit": 1, "timeout": 0.1},
    )
    q = engine.query("SELECT 1", static=True)
    rows = q.scalars()
    with pytest.raises(ConnectionLimitError):
        q.scalar()
    assert list(rows) == [1]
    assert q.scalar() == 1


def test_early_exit_with_block(eng):
    before = idle(eng)
    with eng.query(SELECT, static=True).iter() as rows:
        assert next(rows) == (1, "n1")
        assert idle(eng) == before - 1
    assert idle(eng) == before
    assert list(rows) == []


def test_close_is_idempotent(eng):
    before = idle(eng)
    rows = eng.query(SELECT, static=True).iter()
    rows.close()
    rows.close()
    assert idle(eng) == before
    assert created(eng) <= 2


def test_gc_releases_connection(eng):
    before = idle(eng)
    rows = eng.query(SELECT, static=True).iter()
    next(rows)
    assert idle(eng) == before - 1
    del rows
    gc.collect()
    assert idle(eng) == before


def test_inside_transaction_uses_tx_conn(eng):
    q = eng.query(SELECT, static=True)
    with eng.transaction():
        conn = eng.current.conn
        eng.query(
            "INSERT INTO t (id, title) VALUES (6, ?)",
            static=True,
        ).execute(("n6",))
        rows = q.iter()
        scalars = eng.query(SELECT, static=True).scalars()
        assert rows._release is None
        assert scalars._release is None
        assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6]
        assert list(scalars) == [1, 2, 3, 4, 5, 6]
        rows.close()
        assert eng.current.conn is conn
        assert q.scalar() == 1
    assert eng.current.conn is None


def test_transaction_while_iterating_outside_scope(eng):
    rows = eng.query(SELECT, static=True).iter(batch=1)
    assert next(rows) == (1, "n1")
    with eng.transaction():
        assert eng.current.conn is not rows._cursor.connection
        assert eng.query("SELECT COUNT(*) FROM t", static=True).scalar() == 5
    assert eng.current.conn is None
    assert [r[0] for r in rows] == [2, 3, 4, 5]
    assert idle(eng) == created(eng)


def test_mapper_iter_outside_scope(eng):
    q = eng.query(SELECT_MAPPED).map_to(
        Item,
        "item",
        item=Entity(Item, "id"),
    )
    before = idle(eng)
    rows = q.iter(batch=2)
    assert idle(eng) == before - 1
    assert eng.current.conn is None
    assert [i.id for i in rows] == [1, 2, 3, 4, 5]
    assert idle(eng) == before


def test_mapper_iter_early_exit_and_sql_error(eng):
    q = eng.query(SELECT_MAPPED).map_to(
        Item,
        "item",
        item=Entity(Item, "id"),
    )
    before = idle(eng)
    with q.iter(batch=1) as rows:
        assert next(rows).id == 1
        assert idle(eng) == before - 1
    assert idle(eng) == before
    with pytest.raises(sqlite3.OperationalError):
        eng.query("SELECT * FROM missing").map_to(
            Item,
            "item",
            item=Entity(Item, "id"),
        ).iter()
    assert idle(eng) == before


def test_execute_outside_scope_returns_detached(eng):
    before = idle(eng)
    cur = eng.query(
        "INSERT INTO t (id, title) VALUES (10, ?)",
        static=True,
    ).execute(("x",))
    assert isinstance(cur, DetachedCursor)
    assert cur.rowcount == 1
    assert idle(eng) == before
    assert eng.query("SELECT COUNT(*) FROM t", static=True).scalar() == 6

    cur = eng.query(
        "UPDATE t SET title = ? WHERE id < 4",
        static=True,
    ).execute(("u",))
    assert cur.rowcount == 3

    cur = eng.query(SELECT, static=True).execute()
    for attr in ("description", "lastrowid", "arraysize"):
        with pytest.raises(CursorOutsideScopeError):
            getattr(cur, attr)
    for call in (
        cur.fetchone,
        cur.fetchall,
        lambda: cur.fetchmany(1),
        lambda: iter(cur),
        lambda: cur.execute("SELECT 1"),
        lambda: cur.executemany("SELECT 1", []),
    ):
        with pytest.raises(CursorOutsideScopeError):
            call()
    cur.close()
    assert copy.copy(cur).rowcount == cur.rowcount
    assert not hasattr(cur, "__nope__")


def test_executemany_outside_scope_returns_detached(eng):
    cur = eng.query(
        "INSERT INTO t (id, title) VALUES (:id, :title)",
        static=True,
    ).executemany([{"id": 20, "title": "a"}, {"id": 21, "title": "b"}])
    assert isinstance(cur, DetachedCursor)
    assert (
        eng.query(
            "SELECT COUNT(*) FROM t WHERE id >= 20",
            static=True,
        ).scalar()
        == 2
    )


def test_execute_inside_scope_returns_real_cursor(eng):
    with eng.conn():
        cur = eng.query(SELECT, static=True).execute()
        assert not isinstance(cur, DetachedCursor)
        assert cur.fetchone() == (1, "n1")
