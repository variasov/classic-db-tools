from classic.db_tools import Engine, Entity

from .dto import Task


mapper_sources = '''def mapper_func(rows):
    task_map = {}

    def map_task(row):
        if row[0] is None:
            return (None, None)
        task_id = row[0]
        obj_with_rels = task_map.get(task_id)
        if obj_with_rels is None:
            task = Task(id=row[0])
            task_map[task_id] = (task,)
        else:
            (task,) = obj_with_rels
        return (task, task_id)
    last_obj = None
    for row_ in rows:
        (obj, _) = map_task(row_)
        if last_obj is not obj:
            if last_obj is not None:
                yield last_obj
            last_obj = obj
    if last_obj is not None:
        yield last_obj'''


def test__mapper__sources(engine: Engine):
    query = engine.query(
        'SELECT 1 AS task__id'
    ).map_to(Task, task=Entity(Task, 'id'))
    assert query.sources() == mapper_sources
