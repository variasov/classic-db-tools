import pytest

pytestmark = pytest.mark.usefixtures('data')


class TestTransaction:

    def test_commits_on_success(self, engine):
        engine.query_from('insert_item.sql').execute(t='tc_commit', v=1)
        row = engine.query_from('select_item_by_title.sql').one(t='tc_commit')
        assert row is not None

    def test_rollback_on_exception(self, engine):
        try:
            with engine.transaction():
                engine.query_from('insert_item.sql').execute(
                    t='tc_rollback', v=1,
                )
                raise RuntimeError('boom')
        except RuntimeError:
            pass
        row = engine.query_from(
            'select_item_by_title.sql').one(t='tc_rollback')
        assert row is None

    def test_commit_false_rolls_back(self, engine):
        with engine.transaction(commit=False):
            engine.query_from('insert_item.sql').execute(t='tc_nocommit', v=1)
        row = engine.query_from(
            'select_item_by_title.sql').one(t='tc_nocommit')
        assert row is None

    def test_as_decorator(self, engine):
        call_count = 0

        @engine.transaction
        def work():
            nonlocal call_count
            call_count += 1
            engine.query('SELECT 1', static=True).execute()

        work()
        assert call_count == 1

    def test_decorator_preserves_return_value(self, engine):

        @engine.transaction
        def work():
            return 42

        assert work() == 42


class TestNestedTransaction:

    def test_nested_commit_all(self, engine):
        with engine.transaction():
            engine.query_from('insert_item.sql').execute(
                t='nest_outer', v=1,
            )
            with engine.transaction():
                engine.query_from('insert_item.sql').execute(
                    t='nest_inner', v=2,
                )
        row_out = engine.query_from(
            'select_item_by_title.sql').one(t='nest_outer')
        row_in = engine.query_from(
            'select_item_by_title.sql').one(t='nest_inner')
        assert row_out is not None
        assert row_in is not None

    def test_inner_rollback_does_not_affect_outer(self, engine):
        with engine.transaction():
            engine.query_from('insert_item.sql').execute(t='ni_outer', v=1)
            try:
                with engine.transaction():
                    engine.query_from('insert_item.sql').execute(
                        t='ni_inner', v=2,
                    )
                    raise RuntimeError('inner fail')
            except RuntimeError:
                pass
            row_out = engine.query_from(
                'select_item_by_title.sql').one(t='ni_outer')
            assert row_out is not None
        row_in = engine.query_from(
            'select_item_by_title.sql').one(t='ni_inner')
        assert row_in is None

    def test_outer_rollback_rolls_back_inner_too(self, engine):
        try:
            with engine.transaction():
                engine.query_from('insert_item.sql').execute(
                    t='no_outer', v=1,
                )
                with engine.transaction():
                    engine.query_from('insert_item.sql').execute(
                        t='no_inner', v=2,
                    )
                raise RuntimeError('outer fail')
        except RuntimeError:
            pass
        row_out = engine.query_from(
            'select_item_by_title.sql').one(t='no_outer')
        row_in = engine.query_from(
            'select_item_by_title.sql').one(t='no_inner')
        assert row_out is None
        assert row_in is None

    def test_work_after_inner_rollback(self, engine):
        with engine.transaction():
            engine.query_from('insert_item.sql').execute(t='nw_outer', v=1)
            try:
                with engine.transaction():
                    engine.query_from('insert_item.sql').execute(
                        t='nw_inner', v=2,
                    )
                    raise RuntimeError('inner fail')
            except RuntimeError:
                pass
            engine.query_from('insert_item.sql').execute(
                t='nw_after', v=3,
            )
        row_outer = engine.query_from(
            'select_item_by_title.sql').one(t='nw_outer')
        row_after = engine.query_from(
            'select_item_by_title.sql').one(t='nw_after')
        row_inner = engine.query_from(
            'select_item_by_title.sql').one(t='nw_inner')
        assert row_outer is not None
        assert row_after is not None
        assert row_inner is None

    def test_nested_decorator(self, engine):

        @engine.transaction
        def outer():
            engine.query_from('insert_item.sql').execute(t='nd_outer', v=1)

            @engine.transaction
            def inner():
                engine.query_from('insert_item.sql').execute(
                    t='nd_inner', v=2,
                )

            inner()

        outer()
        assert engine.query_from(
            'select_item_by_title.sql').one(t='nd_outer') is not None
        assert engine.query_from(
            'select_item_by_title.sql').one(t='nd_inner') is not None

    def test_nested_decorator_rollback(self, engine):

        @engine.transaction
        def outer():
            engine.query_from('insert_item.sql').execute(
                t='ndr_outer', v=1,
            )
            raise RuntimeError('outer fail')

        pytest.raises(RuntimeError, outer)
        assert engine.query_from(
            'select_item_by_title.sql').one(t='ndr_outer') is None


class TestConn:

    def test_context_manager(self, engine):
        with engine.conn():
            result = engine.query('SELECT 2 AS b', static=True).scalar()
        assert result == 2

    def test_as_decorator(self, engine):
        call_count = 0

        @engine.conn
        def work():
            nonlocal call_count
            call_count += 1
            engine.query('SELECT 3', static=True).execute()

        work()
        assert call_count == 1

    def test_multiple_queries_in_conn(self, engine):
        with engine.conn():
            engine.query_from('insert_item.sql').execute(t='cm_multi', v=1)
            row = engine.query_from(
                'select_item_by_title.sql').one(t='cm_multi')
            assert row is not None

    def test_decorator_preserves_return_value(self, engine):

        @engine.conn
        def work():
            return 99

        assert work() == 99
