from dataclasses import dataclass

from classic.db_tools import Engine, Mapper, Value
from classic.domain import criteria


@dataclass
class Task:
    id: int
    payload: str

    @criteria
    def is_ready(self):
        pass

    @criteria
    def payload_greater_than(self, payload: int):
        pass


mapper = Mapper(task=Value(Task))

def test_render_criteria(engine: Engine):
    crit = Task.is_ready() & Task.payload_greater_than(payload=1)
    objects = engine.query_from(
        'tasks/find.sql.tmpl'
    ).map_to(
        Task, mapper=mapper,
    ).all(criteria=crit)

    assert objects == [Task(2, '12345')]
