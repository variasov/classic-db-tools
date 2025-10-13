import ast
from typing import Iterable, Generator

from .context import Context
from .params import OneToMany, OneToOne


def render_columns(ctx: Context, col_offset: int) -> Iterable[ast.stmt]:
    for index, column in enumerate(ctx.columns):
        yield ast.Assign(
            targets=[ast.Name(id=column, ctx=ast.Store())],
            value=ast.Constant(value=index),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )

def render_identity_maps(ctx: Context, col_offset: int) -> Iterable[ast.stmt]:
    for mapper in ctx.mappers.values():
        yield ast.Assign(
            targets=[
                ast.Name(id=mapper.identity_map_name, ctx=ast.Store())
            ],
            value=ast.Dict(keys=[], values=[]),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )

def render_last_root(ctx: Context, col_offset: int) -> Iterable[ast.stmt]:
    if not ctx.result_is_unary:
        return []
    return [
        ast.Assign(
            targets=[
                ast.Name(id=ctx.result_mappers[0].last_obj_name, ctx=ast.Store())
            ],
            value=ast.Constant(value=None),
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )]

def render_cycle(ctx: Context, col_offset: int) -> ast.stmt:
    return ast.While(
        test=ast.Constant(value=True),
        body=list(render_cycle_body(ctx, col_offset + 1)),
        orelse=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )

def render_input(ctx: Context, col_offset: int) -> ast.stmt:
    return ast.Assign(
        targets=[ast.Name(id='row', ctx=ast.Store())],
        value=ast.Yield(),
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )

def render_break(ctx: Context, col_offset: int) -> ast.stmt:
    return ast.If(
        test=ast.Compare(
            left=ast.Name(id='row', ctx=ast.Load()),
            ops=[ast.Is()],
            comparators=[ast.Constant(value=None)],
            lineno=ctx.lineno(),
            col_offset=col_offset,
        ),
        body=[ast.Break()],
        orelse=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )

def render_cycle_body(ctx: Context, col_offset: int) -> Generator[ast.stmt, None, None]:

    yield render_input(ctx, col_offset)
    yield render_break(ctx, col_offset)

    for mapper in ctx.mappers.values():
        assign_id_lineno = ctx.lineno()
        # id = row[0]
        yield ast.Assign(
            targets=[ast.Name(id=mapper.id_name, ctx=ast.Store())],
            value=ast.Tuple(
                elts=[
                    ast.Subscript(
                        value=ast.Name(id='row', ctx=ast.Load()),
                        slice=ast.Name(
                            id=ctx.fields_to_columns[mapper][field],
                            ctx=ast.Load(),
                        ),
                        ctx=ast.Load(),
                        lineno=ctx.lineno(),
                        col_offset=col_offset,
                    )
                    for field in mapper.id.fields
                ],
                ctx=ast.Load(),
                lineno=ctx.lineno(),
                col_offset=col_offset,
            ),
            lineno=assign_id_lineno,
            col_offset=col_offset,
        )

        # obj = identity_map.get(id)
        search_obj_lineno = ctx.lineno()
        yield ast.Assign(
            targets=[
                ast.Name(id=mapper.name, ctx=ast.Store())
            ],
            value=ast.Call(
                func=ast.Attribute(
                    value=ast.Name(
                        id=mapper.identity_map_name,
                        ctx=ast.Load(),
                    ),
                    attr='get',
                    ctx=ast.Load()
                ),
                args=[ast.Name(id=mapper.id_name, ctx=ast.Load())],
                keywords=[],
                lineno=search_obj_lineno,
                col_offset=col_offset,
            ),
            lineno=search_obj_lineno,
            col_offset=col_offset,
        )

        # if not obj:
        #     obj = identity_map[id] = cls(
        #         field1=row[1],
        #     )
        if_lineno = ctx.lineno()
        if_body_lineno = ctx.lineno()
        if_body = []

        factory_call = ast.Call(
            func=ast.Name(id=mapper.cls.__name__, ctx=ast.Load()),
            args=[],
            keywords=[
                ast.keyword(
                    arg=field,
                    value=(
                        ast.Subscript(
                            value=ast.Name(id='row', ctx=ast.Load()),
                            slice=ast.Name(id=column, ctx=ast.Load()),
                            ctx=ast.Load(),
                            lineno=ctx.lineno(),
                            col_offset=col_offset + 1,
                        )
                    ),
                    lineno=ctx.lineno(),
                    col_offset=col_offset + 1,
                )
                for field, column
                in ctx.fields_to_columns[mapper].items()
            ],
            lineno=if_body_lineno,
            col_offset=col_offset,
        )
        assign_obj = ast.Assign(
            targets=[
                ast.Name(id=mapper.name, ctx=ast.Store()),
                ast.Subscript(
                    value=ast.Name(
                        id=mapper.identity_map_name,
                        ctx=ast.Load(),
                    ),
                    slice=ast.Name(id=mapper.id_name, ctx=ast.Load()),
                    ctx=ast.Store()
                ),
            ],
            value=factory_call,
            lineno=if_body_lineno,
            col_offset=col_offset,
        )
        if_body.append(assign_obj)

        ctx.lineno()

        if ctx.result_is_unary and mapper is ctx.result_mappers[0]:
            if_body.append(
                ast.If(
                    test=ast.Compare(
                        left=ast.Name(
                            id=mapper.last_obj_name,
                            ctx=ast.Load(),
                        ),
                        ops=[ast.IsNot()],
                        comparators=[ast.Constant(value=None)]),
                    body=[
                        ast.Expr(
                            value=ast.Yield(
                                value=ast.Name(
                                    id=mapper.last_obj_name,
                                    ctx=ast.Load(),
                                )
                            ),
                            lineno=ctx.lineno(),
                            col_offset=col_offset + 1,
                        ),
                    ],
                    orelse=[],
                    lineno=ctx.lineno(),
                    col_offset=col_offset + 1,
                )
            )
            if_body.append(
                ast.Assign(
                    targets=[
                        ast.Name(id=mapper.last_obj_name, ctx=ast.Store())
                    ],
                    value=ast.Name(id=mapper.name, ctx=ast.Load()),
                    lineno=ctx.lineno(),
                    col_offset=col_offset + 1,
                )
            )

        for relationship in ctx.rels[mapper.name]:
            if mapper.accessor_type == 'attr':
                if isinstance(relationship, OneToOne):
                    if_body.append(
                        ast.Assign(
                            targets=[
                                ast.Attribute(
                                    value=ast.Name(
                                        id=ctx.mapper(relationship.left).name,
                                        ctx=ast.Load(),
                                    ),
                                    attr=relationship.field,
                                    ctx=ast.Store(),
                                ),
                            ],
                            value=ast.Name(
                                id=ctx.mapper(relationship.right).name,
                                ctx=ast.Load(),
                            ),
                            lineno=ctx.lineno(),
                            col_offset=col_offset,
                        )
                    )
                elif isinstance(relationship, OneToMany):
                    if_body.append(
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Attribute(
                                        value=ast.Name(
                                            id=ctx.mapper(relationship.left).name,
                                            ctx=ast.Load(),
                                        ),
                                        attr=relationship.field,
                                        ctx=ast.Load()
                                    ),
                                    attr='append',
                                    ctx=ast.Load()
                                ),
                                args=[ast.Name(
                                    id=ctx.mapper(relationship.right).name,
                                    ctx=ast.Load(),
                                )],
                                keywords=[],
                            ),
                            lineno=ctx.lineno(),
                            col_offset=col_offset,
                        )
                    )
                else:
                    raise NotImplemented
            elif mapper.accessor_type == 'item':
                if_body.append(
                    ast.If(
                        test=ast.Compare(
                            left=ast.Constant(value=relationship.field),
                            ops=[ast.NotIn()],
                            comparators=[
                                ast.Name(id=ctx.mapper(relationship.left).name, ctx=ast.Load())
                            ],
                        ),
                        body=[
                            ast.Assign(
                                targets=[
                                    ast.Subscript(
                                        value=ast.Name(
                                            id=ctx.mapper(relationship.left).name,
                                            ctx=ast.Load(),
                                        ),
                                        slice=ast.Constant(
                                            value=relationship.field,
                                        ),
                                        ctx=ast.Store()
                                    ),
                                ],
                                value=ast.List(elts=[], ctx=ast.Load()),
                                lineno=if_body_lineno,
                                col_offset=col_offset,
                            )
                        ],
                        orelse=[],
                        lineno=ctx.lineno(),
                        col_offset=col_offset,
                    )
                )
                if isinstance(relationship, OneToOne):
                    if_body.append(
                        ast.Assign(
                            targets=[
                                ast.Subscript(
                                    value=ast.Name(
                                        id=ctx.mapper(relationship.left).name,
                                        ctx=ast.Load(),
                                    ),
                                    slice=ast.Constant(
                                        value=relationship.field,
                                    ),
                                    ctx=ast.Store(),
                                ),
                            ],
                            value=ast.Name(
                                id=ctx.mapper(relationship.right).name,
                                ctx=ast.Load(),
                            ),
                            lineno=ctx.lineno(),
                            col_offset=col_offset,
                        )
                    )
                elif isinstance(relationship, OneToMany):
                    if_body.append(
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Subscript(
                                        value=ast.Name(
                                            id=ctx.mapper(relationship.left).name,
                                            ctx=ast.Load(),
                                        ),
                                        slice=ast.Constant(
                                            value=relationship.field,
                                        ),
                                        ctx=ast.Load(),
                                    ),
                                    attr='append',
                                    ctx=ast.Load()
                                ),
                                args=[ast.Name(
                                    id=ctx.mapper(relationship.right).name,
                                    ctx=ast.Load(),
                                )],
                                keywords=[],
                            ),
                            lineno=ctx.lineno(),
                            col_offset=col_offset,
                        )
                    )
                else:
                    raise NotImplemented

        yield ast.If(
            test=ast.Compare(
                left=ast.Name(id=mapper.name, ctx=ast.Load()),
                ops=[ast.Is()],
                comparators=[ast.Constant(value=None)]
            ),
            body=if_body,
            orelse=[],
            lineno=if_lineno,
            col_offset=col_offset,
        )

    if not ctx.result_is_unary:
        if len(ctx.mappers) == 1:
            yield_value = ast.Name(
                id=ctx.result_mappers[0].name,
                ctx=ast.Load(),
            )
        else:
            yield_value = ast.Tuple(
                elts=[
                    ast.Name(
                        id=mapper.name,
                        ctx=ast.Load(),
                    )
                    for mapper in ctx.mappers.values()
                ],
                ctx=ast.Load(),
            )
        yield ast.Expr(
            value=ast.Yield(
                value=yield_value,
            ),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )

def render_post_cycle(ctx, col_offset: int) -> Iterable[ast.stmt]:
    if ctx.result_is_unary:
        yield ast.Expr(
            value=ast.Yield(
                value=ast.Name(
                    id=ctx.result_mappers[0].last_obj_name,
                    ctx=ast.Load(),
                ),
            ),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )

def render_mapper_func(ctx: Context, col_offset: int) -> ast.stmt:
    return ast.FunctionDef(
        name='mapper_func',
        args=ast.arguments(
            posonlyargs=[],
            args=[],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=[
            *render_columns(ctx, col_offset),
            *render_identity_maps(ctx, col_offset),
            *render_last_root(ctx, col_offset),
            render_cycle(ctx, col_offset + 1),
            *(render_post_cycle(ctx, col_offset)),
        ],
        decorator_list=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )


def render_module(ctx: Context) -> ast.Module:
    func = render_mapper_func(ctx, 0)
    return ast.fix_missing_locations(
        ast.Module(body=[func], type_ignores=[])
    )
