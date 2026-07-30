from types import SimpleNamespace

import pytest


class TestExecute:

    def test_returns_cursor(self, engine):
        cursor = engine.query_from('get_all.sql').execute()
        row = cursor.fetchone()
        assert row[0] == 1

    def test_with_dict_params(self, engine):
        result = engine.query(
            'SELECT %(v)s AS out', static=True,
        ).scalar({'v': 'dict_val'})
        assert result == 'dict_val'

    def test_with_kwargs(self, engine):
        result = engine.query(
            'SELECT %(v)s AS out', static=True,
        ).scalar(v='kwarg_val')
        assert result == 'kwarg_val'

    def test_with_object_params(self, engine):
        class Obj:
            def __init__(self):
                self.v = 'obj_val'

        result = engine.query(
            'SELECT %(v)s AS out', static=True,
        ).scalar(Obj())
        assert result == 'obj_val'


class TestExecutemany:

    def test_insert_many_dicts(self, engine):
        engine.query_from('insert_item.sql').executemany([
            {'t': 'em_a', 'v': 1},
            {'t': 'em_b', 'v': 2},
        ])
        rows = engine.query_from('select_items_order_by_id.sql').all()
        titles = {r[0] for r in rows}
        assert 'em_a' in titles
        assert 'em_b' in titles

    def test_insert_many_objects(self, engine):
        engine.query_from('insert_item.sql').executemany([
            SimpleNamespace(t='emo_x', v=10),
            SimpleNamespace(t='emo_y', v=20),
        ])
        rows = engine.query_from('select_items_order_by_id.sql').all()
        titles = {r[0] for r in rows}
        assert 'emo_x' in titles
        assert 'emo_y' in titles


class TestAll:

    def test_returns_all_rows(self, engine):
        engine.query_from('insert_item.sql').executemany([
            {'t': 'all_a', 'v': 1},
            {'t': 'all_b', 'v': 2},
        ])
        rows = engine.query_from('select_items_order_by_id.sql').all()
        assert len(rows) == 2

    def test_returns_empty_list(self, engine):
        rows = engine.query_from('select_item_by_title.sql').all(t='no_such')
        assert rows == []


class TestOne:

    def test_returns_row(self, engine):
        engine.query_from('insert_item.sql').execute(t='one_only', v=7)
        row = engine.query_from('select_item_title_value_by_title.sql').one(t='one_only')
        assert row is not None
        assert row[0] == 'one_only'

    def test_returns_none_when_empty(self, engine):
        row = engine.query_from('select_item_title_value_by_title.sql').one(t='no_such')
        assert row is None


class TestScalar:

    def test_returns_value(self, engine):
        val = engine.query('SELECT 42', static=True).scalar()
        assert val == 42

    def test_returns_none_when_empty(self, engine):
        val = engine.query_from('select_value_from_items_by_title.sql').scalar(t='no_such')
        assert val is None

    def test_raises_when_empty_with_raising(self, engine):
        with pytest.raises(IndexError):
            engine.query_from('select_value_from_items_by_title.sql').scalar(
                t='no_such', raising=True,
            )


class TestScalars:

    def test_yields_first_column(self, engine):
        engine.query_from('insert_item.sql').executemany([
            {'t': 'sc_a', 'v': 10},
            {'t': 'sc_b', 'v': 20},
        ])
        vals = list(
        engine.query_from('select_value_from_items_order_by_value.sql').scalars(batch=1),
        )
        assert vals == [10, 20]


class TestIter:

    def test_yields_rows_in_batches(self, engine):
        engine.query_from('insert_item.sql').executemany([
            {'t': 'it_a', 'v': 1},
            {'t': 'it_b', 'v': 2},
        ])
        results = list(
            engine.query_from('select_title_from_items_order_by_title.sql').iter(batch=1),
        )
        assert len(results) == 2


class TestRowcount:

    def test_returns_affected_rows(self, engine):
        engine.query_from('insert_item.sql').execute(t='rc', v=0)
        count = engine.query_from('update_item_value_by_title.sql').rowcount(t='rc', v=99)
        assert count == 1

    def test_zero_for_no_match(self, engine):
        count = engine.query_from('update_item_value_by_title.sql').rowcount(t='no_such', v=99)
        assert count == 0


class TestExternalCursor:

    def test_execute_with_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ext0', 5),
            )
            row = engine.query_from('select_item_title_value_by_title.sql').one(
                cursor=cur, t='ext0',
            )
            assert row is not None
            assert row[0] == 'ext0'
        finally:
            if cur is not None:
                cur.execute('DELETE FROM items WHERE title = %s', ('ext0',))
            engine._pool.release(conn)

    def test_one_with_external_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ext1', 1),
            )
            row = engine.query_from('select_item_title_value_by_title.sql').one(
                cursor=cur, t='ext1',
            )
            assert row[0] == 'ext1'
        finally:
            if cur is not None:
                cur.execute('DELETE FROM items WHERE title = %s', ('ext1',))
            engine._pool.release(conn)

    def test_all_with_external_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ea1', 1),
            )
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ea2', 2),
            )
            rows = engine.query_from('select_title_from_items_by_titles.sql').all(
                cursor=cur, t=['ea1'],
            )
            assert len(rows) == 1
        finally:
            if cur is not None:
                cur.execute(
                    "DELETE FROM items WHERE title IN ('ea1', 'ea2')",
                )
            engine._pool.release(conn)

    def test_scalar_with_external_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('es1', 99),
            )
            val = engine.query_from('select_value_from_items_by_title.sql').scalar(
                cursor=cur, t='es1',
            )
            assert val == 99
        finally:
            if cur is not None:
                cur.execute('DELETE FROM items WHERE title = %s', ('es1',))
            engine._pool.release(conn)

    def test_scalars_with_external_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ess_a', 1),
            )
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ess_b', 2),
            )
            vals = list(
                    engine.query_from('select_value_from_items_order_by_value.sql').scalars(
                        cursor=cur,
                    ),
            )
            assert vals == [1, 2]
        finally:
            if cur is not None:
                cur.execute(
                    "DELETE FROM items WHERE title IN ('ess_a', 'ess_b')",
                )
            engine._pool.release(conn)

    def test_iter_with_external_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ei_a', 1),
            )
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('ei_b', 2),
            )
            rows = list(
                    engine.query_from('select_title_from_items_order_by_title.sql').iter(
                        cursor=cur, batch=1,
                    ),
            )
            assert len(rows) == 2
        finally:
            if cur is not None:
                cur.execute(
                    "DELETE FROM items WHERE title IN ('ei_a', 'ei_b')",
                )
            engine._pool.release(conn)

    def test_rowcount_with_external_cursor(self, engine):
        conn = engine._pool.acquire()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO items (title, value) VALUES (%s, %s)',
                ('erc', 0),
            )
            count = engine.query_from('update_item_value_by_title.sql').rowcount(
                cursor=cur, t='erc', v=10,
            )
            assert count == 1
        finally:
            if cur is not None:
                cur.execute('DELETE FROM items WHERE title = %s', ('erc',))
            engine._pool.release(conn)
