from classic.db_tools import Engine, ConnectionPool

from .conftest import SQL_DIR_PATH, create_pool


def test_engine_commits(conn_pool: ConnectionPool) -> None:
    engine = Engine(SQL_DIR_PATH, conn_pool)
    with engine:
        engine.query(
            'CREATE TABLE example(a int, b int)'
        ).execute()
        engine.query(
            'INSERT INTO example(a, b) '
            'VALUES (1, 2), (2, 3), (3, 4)'
        ).execute()

    with engine:
        assert engine.query(
            'SELECT * FROM example'
        ).all() == [(1, 2), (2, 3), (3, 4)]

    with engine:
        engine.query(
            'DROP TABLE example'
        ).execute()


def test_engine_with_autocommit() -> None:
    conn_pool = create_pool(dict(autocommit=True), dict(limit=1))
    engine = Engine(SQL_DIR_PATH, conn_pool, commit_on_exit=False)
    with engine:
        engine.query(
            'CREATE TABLE example(a int, b int)'
        ).execute()
        engine.query(
            'INSERT INTO example(a, b) '
            'VALUES (1, 2), (2, 3), (3, 4)'
        ).execute()

    with engine:
        assert engine.query(
            'SELECT * FROM example'
        ).all() == [(1, 2), (2, 3), (3, 4)]

    with engine:
        engine.query('DROP TABLE example').execute()


def test_engine_with_autocommit_and_tx() -> None:
    conn_pool = create_pool(dict(autocommit=True), dict(limit=1))
    engine = Engine(SQL_DIR_PATH, conn_pool, commit_on_exit=False)
    with engine:
        with engine.transaction():
            engine.query(
                'CREATE TABLE example(a int, b int)'
            ).execute()
            engine.query(
                'INSERT INTO example(a, b) '
                'VALUES (1, 2), (2, 3), (3, 4)'
            ).execute()

    with engine:
        assert engine.query(
            'SELECT * FROM example'
        ).all() == [(1, 2), (2, 3), (3, 4)]

    with engine:
        with engine.transaction():
            engine.query(
                'DROP TABLE example'
            ).execute()
