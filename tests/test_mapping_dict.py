import pytest
from frozendict import frozendict

from classic.db_tools import Engine, Entity, Value, Add


mapper = dict(
    task=Entity(dict, 'id', statuses=Add('status')),
    status=Value(frozendict),
)


sql = '''
    SELECT
        tasks.id            AS Task__id,
        tasks.name          AS Task__name,
        task_status.id      AS Status__id,
        task_status.title   AS Status__title
    FROM tasks
    LEFT JOIN task_status ON task_status.task_id = tasks.id
    ORDER BY tasks.id, task_status.id 
'''


@pytest.fixture
def tasks(engine: Engine, ddl):
    engine.query_from('example/save_task.sql').executemany([
        {'name': 'First', 'value': ''},
        {'name': 'Second', 'value': ''},
        {'name': 'Third', 'value': ''},
        {'name': 'Four', 'value': ''},
    ])
    engine.query_from('example/save_task_statuses.sql').executemany([
        {'title': 'CREATED', 'task_id': 1},
        {'title': 'CREATED', 'task_id': 2},
        {'title': 'CREATED', 'task_id': 3},
        {'title': 'STARTED', 'task_id': 1},
        {'title': 'FINISHED', 'task_id': 1},
    ])
    yield


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__all(engine: Engine, ddl, tasks, static):
    query = engine.query(sql, static=static).map_to(dict, 'task', **mapper)
    assert query.all() == [
        dict(id=1, name='First', statuses={
            frozendict(id=1, title='CREATED'),
            frozendict(id=4, title='STARTED'),
            frozendict(id=5, title='FINISHED'),
        }),
        dict(id=2, name='Second', statuses={
            frozendict(id=2, title='CREATED'),
        }),
        dict(id=3, name='Third', statuses={
            frozendict(id=3, title='CREATED'),
        }),
        dict(id=4, name='Four', statuses=set()),
    ]


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__all__empty(engine: Engine, ddl, tasks, static):
    assert engine.query('''
        SELECT
            1 AS Task__id,
            1 AS Task__name,
            1 AS Status__id,
            1 AS Status__title
        FROM tasks
        WHERE FALSE
    ''', static=static).map_to(dict, 'task', **mapper).all() == []


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__one(engine: Engine, ddl, tasks, static):
    obj = engine.query(sql, static=static).map_to(dict, 'task', **mapper).one()
    assert obj == dict(id=1, name='First', statuses={
        frozendict(id=1, title='CREATED'),
        frozendict(id=4, title='STARTED'),
        frozendict(id=5, title='FINISHED'),
    })
