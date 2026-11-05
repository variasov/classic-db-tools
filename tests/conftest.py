import os
import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from classic.db_tools import Engine
from classic.db_tools.pool import ConnectionPool


def connect() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


@pytest.fixture(scope="session")
def engine() -> Engine[Any]:
    return Engine(
        sqlite3,
        connect,
        templates_dirs=os.path.join(os.path.dirname(__file__), "sql"),
        pool_class=ConnectionPool,
    )


@pytest.fixture
def standalone_engine() -> Engine[Any]:
    return Engine(sqlite3, connect)


@pytest.fixture(scope="session", autouse=True)
def schema(engine: Engine[Any]) -> None:
    with engine.conn():
        engine.query(
            "CREATE TABLE IF NOT EXISTS items "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, value INT)",
            static=True,
        ).execute()
        engine.query(
            "CREATE TABLE IF NOT EXISTS _map_items "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, value INT)",
            static=True,
        ).execute()
        engine.query(
            "CREATE TABLE IF NOT EXISTS _map_parents "
            "(id INT PRIMARY KEY, name TEXT)",
            static=True,
        ).execute()
        engine.query(
            "CREATE TABLE IF NOT EXISTS _map_children "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INT, label TEXT)",
            static=True,
        ).execute()
        engine.query(
            "CREATE TABLE IF NOT EXISTS _map_tags "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INT, name TEXT)",
            static=True,
        ).execute()


@pytest.fixture
def data(engine: Engine[Any], schema: None) -> Iterator[None]:
    with engine.transaction(commit=False):
        yield
