from dataclasses import dataclass

from classic.db_tools import Engine, Entity


@dataclass
class SomeObj:
    field_1: int
    field_2: int
    field_3: int
    field_4: int


def test_composite_keys_mapping(engine: Engine):
    query = engine.query('''
        SELECT
            field_1 as SomeObj__field_1,
            field_2 as SomeObj__field_2,
            field_3 as SomeObj__field_3,
            field_4 as SomeObj__field_4
        FROM (
            VALUES
                (1, 1, 1, 1),
                (1, 1, 2, 2),
                (1, 2, 3, 3),
                (1, 3, 4, 4)
        ) AS data(field_1, field_2, field_3, field_4)
    ''').map_to(
        SomeObj,
        SomeObj=Entity(SomeObj, ('field_1', 'field_2')),
    )
    assert query.all() == [
        SomeObj(1, 1, 1, 1),
        SomeObj(1, 2, 3, 3),
        SomeObj(1, 3, 4, 4),
    ]
