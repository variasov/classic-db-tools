from unittest.mock import Mock, MagicMock

from classic.sql_tools.result import Result
import pytest


@pytest.fixture
def cursor_with_many_rows():
    cursor = Mock()
    cursor.rowcount = 2
    cursor.fetchall = MagicMock(return_value=[(1, 1), (2, 2)])
    cursor.fetchmany = MagicMock(return_value=[(1, 1), (2, 2)])
    cursor.fetchone = MagicMock(return_value=(1, 1))
    return cursor


@pytest.fixture
def cursor_with_one_row():
    cursor = Mock()
    cursor.rowcount = 1
    cursor.fetchall = MagicMock(return_value=[(1, 1)])
    cursor.fetchmany = MagicMock(return_value=[(1, 1)])
    cursor.fetchone = MagicMock(return_value=(1, 1))
    return cursor


@pytest.fixture
def empty_cursor():
    cursor = Mock()
    cursor.rowcount = 0
    cursor.fetchone = MagicMock(return_value=None)
    cursor.fetchmany = MagicMock(return_value=[])
    cursor.fetchall = MagicMock(return_value=[])
    return cursor


@pytest.fixture
def result_many_row(cursor_with_many_rows):
    return Result(cursor_with_many_rows)


@pytest.fixture
def result_one_row(cursor_with_one_row):
    return Result(cursor_with_one_row)


@pytest.fixture
def result_empty(empty_cursor):
    return Result(empty_cursor)


@pytest.mark.parametrize('result,value,batch', [
    ('result_many_row', [(1, 1), (2, 2)], None),
    ('result_one_row', [(1, 1)], None),
    ('result_empty', [], None),
    ('result_many_row', [(1, 1), (2, 2)], 10),
    ('result_one_row', [(1, 1)], 10),
    ('result_empty', [], 10),
])
def test_many(request, result, value, batch):
    result = request.getfixturevalue(result)
    assert result.many(batch) == value


@pytest.mark.parametrize('result,value,exc,raising', [
    ('result_many_row', (1, 1), None, False),
    ('result_one_row', (1, 1), None, False),
    ('result_empty', None, None, False),
    ('result_empty', None, ValueError, True),
])
def test_one(request, result, value, exc, raising):
    result = request.getfixturevalue(result)
    if exc:
        with pytest.raises(exc):
            result.one(raising)
    else:
        assert result.one(raising) == value


@pytest.mark.parametrize('result,value,exc,raising', [
    ('result_many_row', 1, None, False),
    ('result_one_row', 1, None, False),
    ('result_empty', None, ValueError, True),
])
def test_scalar(request, result, value, exc, raising):
    result = request.getfixturevalue(result)
    if exc:
        with pytest.raises(exc):
            result.scalar(raising)
    else:
        assert result.scalar(raising) == value
