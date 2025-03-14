from abc import ABC, abstractmethod
from ast import *
from typing import Sequence


class Mapper(ABC):
    name: str
    identity_map_name: str

    @abstractmethod
    def to_ast(self):
        pass


class ToDict(Mapper):

    def __init__(
        self,
        name: str,
        id: str | Sequence[str],
        cols: dict[str, str],
    ):
        self.name = name
        self.identity_map_name = self.name + 's'
        self.id = id
        self.cols = cols

    def to_ast(self):
        return [
            Assign(
                targets=[Name(id=self.name, ctx=Store())],
                value=Call(
                    func=Attribute(
                        value=Name(id=self.identity_map_name, ctx=Load()),
                        attr='get',
                        ctx=Load(),
                    ),
                    args=[
                        Subscript(
                            value=Name(id='row', ctx=Load()),
                            slice=Constant(value=self.id),
                            ctx=Load(),
                        )
                    ],
                    keywords=[],
                )
            ),
        ]


class OneToMany:

    def __init__(
        self,
        left: str,
        attr: str,
        right: str,
    ):
        self.left = left
        self.attr = attr
        self.right = right

    def to_ast(self):
        return Expr(
            value=Call(
                func=Attribute(
                    value=Subscript(
                        value=Name(id=self.left, ctx=Load()),
                        slice=Constant(value=self.attr),
                        ctx=Load(),
                    ),
                    attr='append',
                    ctx=Load()
                ),
                args=[Name(id=self.right, ctx=Load())],
                keywords=[],
            )
        )


def returning(*mappers, returns: str):
    identity_maps = []
    for obj in mappers:
        if isinstance(obj, Mapper):
            identity_maps.append(
                Assign(
                    targets=[Name(id=obj.identity_map_name, ctx=Store())],
                    value=Dict(keys=[], values=[]),
                )
            )

    cycle_body = []
    for obj in mappers:
        cycle_body.append(obj.to_ast())

    cycle = For(
        target=Name(id='row', ctx=Store()),
        iter=Name(id='rows', ctx=Load()),
        body=cycle_body,
        orelse=[],
    )

    return_result = Return(value=Name(id=returns, ctx=Load()))

    func = FunctionDef(
        name='mapper_func',
        args=arguments(
            posonlyargs=[], args=[arg(arg='rows')], kwonlyargs=[],
            kw_defaults=[], defaults=[],
        ),
        body=[
            *identity_maps,
            cycle,
            return_result,
        ],
        decorator_list=[],
        lineno=1,
        col_offset=0,
    )

    # src = unparse(fix_missing_locations(func))

    module = Module(body=[fix_missing_locations(func)], type_ignores=[])

    code = compile(
        module, '<string>', 'exec',
        # src, '<string>', 'exec',
    )
    namespace = {}
    exec(code, namespace)
    return namespace['mapper_func']
