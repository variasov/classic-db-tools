import ast
from itertools import chain
from typing import Iterable, Generator

from .types import Accessor
from .context import Context, Mapper
from .params import OneToMany, OneToOne, Relationship


def render_identity_maps(ctx: Context, col_offset: int) -> Iterable[ast.stmt]:
    for mapper in ctx.mappers.values():
        yield ast.Assign(
            targets=[
                ast.Name(id=mapper.id_map_name, ctx=ast.Store())
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
                ast.Name(
                    id='last_root',
                    ctx=ast.Store(),
                )
            ],
            value=ast.Constant(value=None),
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )]


def render_cycle(ctx: Context, col_offset: int) -> ast.stmt:
    return ast.For(
        target=ast.Name(id='row', ctx=ast.Store()),
        iter=ast.Name(id='rows', ctx=ast.Load()),
        body=list(render_cycle_body(ctx, col_offset + 1)),
        orelse=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )


def render_check_for_none(
    ctx: Context, col_offset: int, columns: list[int],
) -> ast.stmt:
    if len(columns) == 1:
        stmt = ast.Compare(
            left=ast.Subscript(
                value=ast.Name(id='row', ctx=ast.Load()),
                slice=ast.Constant(value=columns[0]),
                ctx=ast.Load(),
            ),
            ops=[ast.Is()],
            comparators=[ast.Constant(value=None)],
        )
    else:
        stmt = ast.BoolOp(
            op=ast.And(),
            values=[
                ast.Compare(
                    left=ast.Subscript(
                        value=ast.Name(id='row', ctx=ast.Load()),
                        slice=ast.Constant(value=column),
                        ctx=ast.Load(),
                    ),
                    ops=[ast.Is()],
                    comparators=[ast.Constant(value=None)],
                ) for column in columns
            ]
        )
    return ast.If(
        test=stmt,
        body=[
            ast.Return(
                ast.Constant(value=None),
                lineno=ctx.lineno(),
                col_offset=col_offset + 1,
            )
        ],
        orelse=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )


def render_factory_call(ctx: Context, col_offset: int, mapper: Mapper) -> ast.Assign:
    return ast.Assign(
        targets=[
            ast.Name(id=mapper.name, ctx=ast.Store()),
        ],
        value=ast.Call(
            func=ast.Name(id=mapper.cls.__name__, ctx=ast.Load()),
            args=[],
            keywords=[
                ast.keyword(
                    arg=field,
                    value=(
                        ast.Subscript(
                            value=ast.Name(id='row', ctx=ast.Load()),
                            slice=ast.Constant(value=column[0]),
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
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )
    )


def render_mapper_call(ctx: Context, col_offset: int, mapper: Mapper) -> ast.expr:
    return ast.Call(
        func=ast.Name(
            id=mapper.func_name,
            ctx=ast.Load()
        ),
        args=[ast.Name(id='row', ctx=ast.Load())],
        keywords=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )


def render_rel_factory_call(
    ctx: Context, col_offset: int,
    rel: Relationship,
    accessor: Accessor,
) -> Generator[ast.stmt, None, None]:
    if isinstance(rel, OneToOne):
        target = ast.Attribute(
            value=ast.Name(
                id=ctx.mapper(rel.left).name,
                ctx=ast.Load(),
            ),
            attr=rel.field,
            ctx=ast.Store(),
        )  if accessor == 'attr' else ast.Subscript(
            value=ast.Name(
                id=ctx.mapper(rel.left).name,
                ctx=ast.Load(),
            ),
            slice=ast.Constant(
                value=rel.field,
            ),
            ctx=ast.Store(),
        )

        yield ast.Assign(
            targets=[target],
            value=render_mapper_call(
                ctx, col_offset,
                ctx.mapper(rel.right),
            ),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )
    elif isinstance(rel, OneToMany):
        left_mapper = ctx.mapper(rel.left)

        if accessor == 'item':
            yield ast.If(
                test=ast.Compare(
                    left=ast.Constant(value=rel.field),
                    ops=[ast.NotIn()],
                    comparators=[
                        ast.Name(id=left_mapper.name, ctx=ast.Load())
                    ]
                ),
                body=[
                    ast.Assign(
                        targets=[
                            ast.Subscript(
                                value=ast.Name(
                                    id=left_mapper.name, ctx=ast.Load(),
                                ),
                                slice=ast.Constant(value=rel.field),
                                ctx=ast.Store(),
                            ),
                        ],
                        value=ast.List(elts=[], ctx=ast.Load()),
                        lineno=ctx.lineno(),
                        col_offset=col_offset + 1,
                    )
                ],
                orelse=[],
                lineno=ctx.lineno(),
                col_offset=col_offset,
            )
            value = ast.Subscript(
                value=ast.Name(
                    id=ctx.mapper(rel.left).name,
                    ctx=ast.Load(),
                ),
                slice=ast.Constant(value=rel.field),
                ctx=ast.Load(),
            )
        else:
            value = ast.Attribute(
                value=ast.Name(
                    id=ctx.mapper(rel.left).name,
                    ctx=ast.Load(),
                ),
                attr=rel.field,
                ctx=ast.Load(),
            )

        yield ast.If(
            test=ast.NamedExpr(
                target=ast.Name(id='obj', ctx=ast.Store()),
                value=ast.Call(
                    func=ast.Name(
                        id=ctx.mapper(rel.right).func_name,
                        ctx=ast.Load(),
                    ),
                    args=[ast.Name(id='row', ctx=ast.Load())],
                    keywords=[]
                ),
            ),
            body=[
                ast.Expr(
                    value=ast.Call(
                        func=ast.Attribute(
                            value=value,
                            attr='append',
                            ctx=ast.Load()
                        ),
                        args=[
                            render_mapper_call(
                                ctx, col_offset,
                                ctx.mapper(rel.right),
                            )
                        ],
                        keywords=[],
                    ),
                    lineno=ctx.lineno(),
                    col_offset=col_offset + 1,
                )
            ],
            orelse=[],
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )
    else:
        raise NotImplemented


def render_obj_id(ctx: Context, col_offset: int, mapper: Mapper) -> ast.stmt:
    lineno = ctx.lineno()
    if len(mapper.id.fields) == 1:
        id_field = mapper.id.fields[0]
        id_col = ctx.fields_to_columns[mapper][id_field][0]
        id_val = ast.Subscript(
            value=ast.Name(id='row', ctx=ast.Load()),
            slice=ast.Constant(value=id_col),
            ctx=ast.Load(),
            lineno=lineno,
            col_offset=col_offset,
        )
    else:
        id_val = ast.Tuple(
            elts=[
                ast.Subscript(
                    value=ast.Name(id='row', ctx=ast.Load()),
                    slice=ast.Constant(
                        value=ctx.fields_to_columns[mapper][field][0],
                    ),
                    ctx=ast.Load(),
                    lineno=lineno,
                    col_offset=col_offset,
                )
                for field in mapper.id.fields
            ],
            ctx=ast.Load(),
            lineno=lineno,
            col_offset=col_offset,
        )

    return ast.Assign(
        targets=[ast.Name(id=mapper.id_name, ctx=ast.Store())],
        value=id_val,
        lineno=lineno,
        col_offset=col_offset,
    )


def render_get_or_create(
    ctx: Context, col_offset: int,
    mapper: Mapper
) -> Generator[ast.stmt, None, None]:
    lineno = ctx.lineno()
    yield ast.Assign(
        targets=[
            ast.Name(id=mapper.name, ctx=ast.Store())
        ],
        value=ast.Call(
            func=ast.Attribute(
                value=ast.Name(
                    id=mapper.id_map_name,
                    ctx=ast.Load(),
                ),
                attr='get',
                ctx=ast.Load()
            ),
            args=[ast.Name(id=mapper.id_name, ctx=ast.Load())],
            keywords=[],
            lineno=lineno,
            col_offset=col_offset,
        ),
        lineno=lineno,
        col_offset=col_offset,
    )

    assign = render_factory_call(ctx, col_offset, mapper)
    assign.targets.append(
        ast.Subscript(
            value=ast.Name(
                id=mapper.id_map_name,
                ctx=ast.Load(),
            ),
            slice=ast.Name(id=mapper.id_name, ctx=ast.Load()),
            ctx=ast.Store(),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        ),
    )
    yield ast.If(
        test=ast.Compare(
            left=ast.Name(id=mapper.name, ctx=ast.Load()),
            ops=[ast.Is()],
            comparators=[ast.Constant(value=None)]
        ),
        body=[assign],
        orelse=[],
        lineno=ctx.lineno(),
        col_offset=col_offset,
    )


def render_id_mapper(
    ctx: Context, col_offset: int, mapper: Mapper
) -> ast.stmt:
    id_columns = [
        cols[0]
        for field, cols in ctx.fields_to_columns[mapper].items()
        if field in mapper.id.fields
    ]
    lineno = ctx.lineno()
    return ast.FunctionDef(
        name=mapper.func_name,
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg='rows'),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=[
            render_check_for_none(ctx, col_offset + 1, id_columns),
            render_obj_id(ctx, col_offset + 1, mapper),
            *render_get_or_create(ctx, col_offset + 1, mapper),
            *chain.from_iterable(
                render_rel_factory_call(
                    ctx, col_offset + 1, rel, mapper.accessor,
                )
                for rel in ctx.rels[mapper.name]
            ),
            ast.Return(
                value=ast.Name(id=mapper.name, ctx=ast.Load()),
                lineno=ctx.lineno(),
                col_offset=col_offset + 1,
            )
        ],
        decorator_list=[],
        lineno=lineno,
        col_offset=col_offset,
    )


def render_plain_mapper(
    ctx: Context, col_offset: int, mapper: Mapper,
) -> ast.stmt:
    lineno = ctx.lineno()
    columns = [col[0] for col in ctx.fields_to_columns[mapper].values()]
    body = []
    if mapper.reduce_null:
        body.append(render_check_for_none(ctx, col_offset + 1, columns))
    body += [
        render_factory_call(ctx, col_offset + 1, mapper),
        *chain.from_iterable(
            render_rel_factory_call(
                ctx, col_offset + 1, rel, mapper.accessor,
            )
            for rel in ctx.rels[mapper.name]
        ),
        ast.Return(
            value=ast.Name(id=mapper.name, ctx=ast.Load()),
            lineno=ctx.lineno(),
            col_offset=col_offset + 1,
        ),
    ]
    return ast.FunctionDef(
        name=mapper.func_name,
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg='rows'),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=body,
        decorator_list=[],
        lineno=lineno,
        col_offset=col_offset,
    )


def render_mappers(
    ctx: Context, col_offset: int
) -> Generator[ast.stmt, None, None]:
    for mapper in ctx.mappers.values():
        if mapper.id:
            yield render_id_mapper(ctx, col_offset, mapper)
        else:
            yield render_plain_mapper(ctx, col_offset, mapper)


def render_cycle_body(
    ctx: Context,
    col_offset: int
) -> Generator[ast.stmt, None, None]:
    if ctx.result_is_unary:
        yield ast.Assign(
            targets=[ast.Name(id='root', ctx=ast.Store())],
            value=render_mapper_call(
                ctx, col_offset,
                ctx.result_mappers[0],
            ),
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )
        yield ast.If(
            test=ast.Compare(
                left=ast.Name(id='last_root', ctx=ast.Load()),
                ops=[ast.IsNot()],
                comparators=[ast.Name(id='root', ctx=ast.Load())],
            ),
            body=[
                ast.If(
                    test=ast.Compare(
                        left=ast.Name(id='last_root', ctx=ast.Load()),
                        ops=[ast.IsNot()],
                        comparators=[ast.Constant(value=None)],
                    ),
                    body=[
                        ast.Expr(
                            ast.Yield(
                                value=ast.Name(id='last_root', ctx=ast.Load()),
                            ),
                        )
                    ],
                    orelse=[],
                ),
                ast.Assign(
                    targets=[ast.Name(id='last_root', ctx=ast.Store())],
                    value=ast.Name(id='root', ctx=ast.Load()),
                    lineno=ctx.lineno(),
                    col_offset=col_offset,
                ),
            ],
            orelse=[],
        )
    else:
        if len(ctx.mappers) == 1:
            yield_value = ast.Name(
                id=ctx.result_mappers[0].name,
                ctx=ast.Load(),
            )
        else:
            yield_value = ast.Tuple(
                elts=[
                    ast.Call(
                        func=ast.Name(mapper.func_name, ast.Load()),
                        args=[ast.Name('row', ast.Load())],
                        keywords=[],
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
        yield ast.If(
            test=ast.Compare(
                left=ast.Name(
                    id='last_root',
                    ctx=ast.Load(),
                ),
                ops=[ast.IsNot()],
                comparators=[ast.Constant(value=None)],
            ),
            body=[
                ast.Expr(
                    ast.Yield(
                        value=ast.Name(
                            id='last_root',
                            ctx=ast.Load(),
                        ),
                    ),
                    lineno=ctx.lineno(),
                    col_offset=col_offset + 1,
                ),
            ],
            orelse=[],
            lineno=ctx.lineno(),
            col_offset=col_offset,
        )


def render_mapper_func(ctx: Context, col_offset: int) -> ast.stmt:
    return ast.FunctionDef(
        name='mapper_func',
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg='rows'),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=[
            *render_identity_maps(ctx, col_offset),
            *render_mappers(ctx, col_offset),
            *render_last_root(ctx, col_offset),
            render_cycle(ctx, col_offset),
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
