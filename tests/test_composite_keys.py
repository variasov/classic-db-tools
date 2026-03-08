# from dataclasses import dataclass
# from typing import Annotated
#
# from classic.db_tools import Engine,
#
#
# @dataclass
# class Nested:
#     field_4: int
#
#
# @dataclass
# class SomeObj:
#     field_1: int
#     field_2: int
#     field_3: int
#
#
# def test_composite_keys_mapping(engine: Engine):
#     query = engine.query('''
#         SELECT
#             field_1 as SomeObj__field_1,
#             field_2 as SomeObj__field_2,
#             field_3 as SomeObj__field_3,
#             field_4 as Nested__field_4
#         FROM (
#             VALUES
#                 (1, 1, 1, 1),
#                 (1, 1, 2, 2),
#                 (1, 2, 3, 3),
#                 (1, 3, 4, 4)
#         ) AS data(field_1, field_2, field_3, field_4)
#     ''').map_to(
#         tuple[
#             Annotated[SomeObj, ID('field_1', 'field_2')],
#             Annotated[Nested, ID('field_4')],
#         ],
#     )
#     assert query.all() == [
#         (SomeObj(1, 1, 1), Nested(1)),
#         (SomeObj(1, 1, 1), Nested(2)),
#         (SomeObj(1, 2, 3), Nested(3)),
#         (SomeObj(1, 3, 4), Nested(4)),
#     ]
