import psycopg.rows

from classic.db_tools import Engine


def test_engine_works_with_outer_cursor(engine: Engine) -> None:
    with engine.conn() as conn:
        cursor: psycopg.Cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        cursor_ = engine.query('SELECT 1 AS res').execute(cursor=cursor)
        assert cursor_.fetchone() == {'res': 1}
