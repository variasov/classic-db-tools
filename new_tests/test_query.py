import psycopg
import pytest


_TABLE = 'DROP TABLE IF EXISTS _qitems CASCADE; CREATE TEMP TABLE _qitems (id SERIAL PRIMARY KEY, title TEXT, value INT)'
_INSERT = 'INSERT INTO _qitems (title, value) VALUES (%(t)s, %(v)s)'
_SELECT_ALL = 'SELECT id, title, value FROM _qitems ORDER BY id'
_SELECT_BY_TITLE = "SELECT title, value FROM _qitems WHERE title = %(t)s"
_SELECT_VAL_BY_TITLE = "SELECT value FROM _qitems WHERE title = %(t)s"
_UPDATE_BY_TITLE = "UPDATE _qitems SET value = %(v)s WHERE title = %(t)s"


class TestExecute:

    def test_execute_returns_cursor(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            cursor = engine.query('SELECT 1 AS a', static=True).execute()
            row = cursor.fetchone()
            assert row[0] == 1

    def test_execute_with_params_dict(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            result = engine.query('SELECT %(v)s AS out', static=True).scalar({'v': 'dict_param'})
            assert result == 'dict_param'

    def test_execute_with_params_kwargs(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            result = engine.query('SELECT %(v)s AS out', static=True).scalar(v='kwarg_param')
            assert result == 'kwarg_param'

    def test_execute_with_object_params(self, engine):
        class Obj:
            def __init__(self):
                self.v = 'obj_param'
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            result = engine.query('SELECT %(v)s AS out', static=True).scalar(Obj())
            assert result == 'obj_param'


class TestExecutemany:

    def test_insert_many(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).executemany([
                {'t': 'a', 'v': 1},
                {'t': 'b', 'v': 2},
            ])

    def test_executemany_with_objects(self, engine):
        from types import SimpleNamespace
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).executemany([
                SimpleNamespace(t='x', v=10),
                SimpleNamespace(t='y', v=20),
            ])
            rows = engine.query('SELECT title, value FROM _qitems ORDER BY title', static=True).all()
            assert len(rows) == 2


class TestAll:

    def test_all_returns_list(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).executemany([{'t': 'a', 'v': 1}, {'t': 'b', 'v': 2}])
            rows = engine.query('SELECT title, value FROM _qitems ORDER BY title', static=True).all()
            assert len(rows) == 2


class TestOne:

    def test_one_returns_row(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).execute(t='only', v=7)
            row = engine.query('SELECT title, value FROM _qitems', static=True).one()
            assert row is not None
            assert row[0] == 'only'

    def test_one_returns_none_when_empty(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            row = engine.query('SELECT * FROM _qitems', static=True).one()
            assert row is None


class TestScalar:

    def test_scalar_returns_value(self, engine):
        with engine.transaction():
            val = engine.query('SELECT 42', static=True).scalar()
            assert val == 42

    def test_scalar_returns_none_when_empty(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            val = engine.query('SELECT value FROM _qitems WHERE id = -1', static=True).scalar()
            assert val is None

    def test_scalar_raising_raises_when_empty(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            with pytest.raises(TypeError):
                engine.query('SELECT value FROM _qitems WHERE id = -1', static=True).scalar(raising=True)


class TestIter:

    def test_iter_yields_rows(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).executemany([{'t': 'a', 'v': 1}, {'t': 'b', 'v': 2}])
            results = list(
                engine.query('SELECT title FROM _qitems ORDER BY title', static=True).iter(batch=1)
            )
            assert len(results) == 2
            assert results[0][0] == 'a'


class TestScalars:

    def test_scalars_yields_first_column(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).executemany([{'t': 'a', 'v': 1}, {'t': 'b', 'v': 2}])
            vals = list(
                engine.query('SELECT value FROM _qitems ORDER BY value', static=True).scalars(batch=1)
            )
            assert vals == [1, 2]


class TestRowcount:

    def test_rowcount_after_update(self, engine):
        with engine.transaction():
            engine.query(_TABLE, static=True).execute()
            engine.query(_INSERT, static=True).execute(t='rc', v=0)
            count = engine.query(_UPDATE_BY_TITLE, static=True).rowcount(t='rc', v=99)
            assert count > 0


class TestExternalCursor:

    def test_execute_with_external_cursor(self, engine):
        conn = psycopg.connect()
        try:
            cur = conn.cursor()
            with conn.transaction():
                cur.execute('CREATE TEMP TABLE _qitems_ext (id SERIAL PRIMARY KEY, title TEXT, value INT)')
                cur.execute('INSERT INTO _qitems_ext (title, value) VALUES (%s, %s)', ('ext', 5))
            row = engine.query(
                'SELECT title, value FROM _qitems_ext WHERE title = %(t)s',
                static=True,
            ).one(cursor=cur, t='ext')
            assert row is not None
            assert row[0] == 'ext'
        finally:
            conn.close()

    def test_one_with_external_cursor(self, engine):
        conn = psycopg.connect()
        try:
            cur = conn.cursor()
            with conn.transaction():
                cur.execute('CREATE TEMP TABLE _qitems_ext1 (id SERIAL PRIMARY KEY, title TEXT, value INT)')
                cur.execute('INSERT INTO _qitems_ext1 (title, value) VALUES (%s, %s)', ('ext1', 1))
            row = engine.query(
                'SELECT title FROM _qitems_ext1 WHERE title = %(t)s',
                static=True,
            ).one(cursor=cur, t='ext1')
            assert row[0] == 'ext1'
        finally:
            conn.close()

    def test_all_with_external_cursor(self, engine):
        conn = psycopg.connect()
        try:
            cur = conn.cursor()
            with conn.transaction():
                cur.execute('CREATE TEMP TABLE _qitems_ext2 (id SERIAL PRIMARY KEY, title TEXT, value INT)')
                cur.execute('INSERT INTO _qitems_ext2 (title, value) VALUES (%s, %s)', ('ea1', 1))
                cur.execute('INSERT INTO _qitems_ext2 (title, value) VALUES (%s, %s)', ('ea2', 2))
            rows = engine.query(
                "SELECT title FROM _qitems_ext2 WHERE title = ANY(%(t)s)", static=True,
            ).all(cursor=cur, t=['ea1'])
            assert len(rows) == 1
        finally:
            conn.close()

    def test_scalar_with_external_cursor(self, engine):
        conn = psycopg.connect()
        try:
            cur = conn.cursor()
            with conn.transaction():
                cur.execute('CREATE TEMP TABLE _qitems_ext3 (id SERIAL PRIMARY KEY, title TEXT, value INT)')
                cur.execute('INSERT INTO _qitems_ext3 (title, value) VALUES (%s, %s)', ('es1', 99))
            val = engine.query(
                'SELECT value FROM _qitems_ext3 WHERE title = %(t)s',
                static=True,
            ).scalar(cursor=cur, t='es1')
            assert val == 99
        finally:
            conn.close()

    def test_rowcount_with_external_cursor(self, engine):
        conn = psycopg.connect()
        try:
            cur = conn.cursor()
            with conn.transaction():
                cur.execute('CREATE TEMP TABLE _qitems_ext4 (id SERIAL PRIMARY KEY, title TEXT, value INT)')
                cur.execute('INSERT INTO _qitems_ext4 (title, value) VALUES (%s, %s)', ('erc', 0))
            count = engine.query(
                'UPDATE _qitems_ext4 SET value = 10 WHERE title = %(t)s',
                static=True,
            ).rowcount(cursor=cur, t='erc')
            assert count == 1
        finally:
            conn.close()