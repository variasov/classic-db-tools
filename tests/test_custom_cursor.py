from typing import cast

import psycopg.rows

from classic.db_tools import Engine, types


def test_engine_works_with_outer_cursor(engine: Engine) -> None:
    with engine.conn() as conn:
        conn = cast(psycopg.Connection, conn)
        cursor: psycopg.Cursor[psycopg.rows.DictRow] = conn.cursor(
            row_factory=psycopg.rows.dict_row,
        )
        cursor_ = cast(types.Cursor, cursor)
        cursor_ = engine.query('SELECT 1 AS res').execute(cursor=cursor_)
        assert cursor_.fetchone() == {'res': 1}
