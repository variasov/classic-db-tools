from classic.db_tools import Engine, Mapper, Entity

from .dto import Task


mapper_sources = '''def mapper_func(rows):
    task_map = {}

    def map_task(rows):
        if row[0] is None:
            return None
        task_id = row[0]
        task = task_map.get(task_id)
        if task is None:
            task = task_map[task_id] = Task(id=row[0])
        return task
    last_root = None
    for row in rows:
        root = map_task(row)
        if last_root is not root:
            if last_root is not None:
                yield last_root
            last_root = root
    if last_root is not None:
        yield last_root'''


def test__mapper__sources(engine: Engine):
    query = engine.query(
        'SELECT 1 AS task__id'
    ).map_to(Task, mapper=Mapper(task=Entity(Task, 'id')))
    assert query.sources() == mapper_sources
