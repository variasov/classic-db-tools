from classic.sql_tools import ToDict, OneToMany, returning


data = [
    {
        "id": 0,
        "name": "Carrot cake",
        "slices": 8,
        "dude_id": 0,
        "dude_name": "Ahsoka"
    },
    {
        "id": 0,
        "name": "Carrot cake",
        "slices": 8,
        "dude_id": 3,
        "dude_name": "CT-7567 Rex"
    }
]

result = [{
    "id": 0,
    "name": "Carrot cake",
    "slices": 8,
    "eaten_by":
        [{"id": 0, "name": "Ahsoka"},
         {"id": 3, "name": "CT-7567 Rex"}]
}]


def test_returning():
    func = returning(
        ToDict('cakes', 'id', {
            'id': 'id',
            'name': 'name',
            'slices': 'slices',
        }),
        ToDict('dudes', 'dude_id', {
            'id': 'dude_id',
            'name': 'dude_name',
        }),
        OneToMany('cake', 'eaten_by', 'dude'),
        returns='cakes'
    )
    assert func(data) == result

from ast import *

# def test_tmp():
#
#     func = FunctionDef(
#         name='mapper_func',
#         args=arguments(
#             posonlyargs=[], args=[arg(arg='rows')], kwonlyargs=[],
#             kw_defaults=[], defaults=[],
#         ),
#         body=[
#             Return(value=Constant(value=1)),
#         ],
#         decorator_list=[],
#         lineno=1,
#         col_offset=0,
#     )

