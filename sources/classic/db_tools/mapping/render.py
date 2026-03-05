import ast
from itertools import chain
from typing import Iterable, Generator, List

from .types import Accessor
from .context import Context
from .params import Relationship, Assign, Append
from .mappers import Mapper, EntityMapper, ValueMapper


def render_identity_maps(ctx: Context) -> Iterable[ast.stmt]:
    for mapper in ctx.mappers.values():
        yield ast.Assign(
            [ast.Name(mapper.id_map_name, ast.Store())],
            ast.Dict(keys=[], values=[]),
        )


def render_last_root(ctx: Context) -> Iterable[ast.stmt]:
    if not ctx.result_is_unary:
        return []
    return [
        ast.Assign(
            [ast.Name("last_root", ast.Store())],
            ast.Constant(None),
        )
    ]


def render_cycle(ctx: Context) -> ast.stmt:
    return ast.For(
        target=ast.Name("row", ast.Store()),
        iter=ast.Name("rows", ast.Load()),
        body=list(render_cycle_body(ctx)),
        orelse=[],
    )


def render_check_for_none(
    columns: List[int],
) -> ast.stmt:
    if len(columns) == 1:
        stmt = ast.Compare(
            left=ast.Subscript(
                ast.Name("row", ast.Load()),
                ast.Constant(columns[0]),
                ast.Load(),
            ),
            ops=[ast.Is()],
            comparators=[ast.Constant(None)],
        )
    else:
        stmt = ast.BoolOp(
            op=ast.And(),
            values=[
                ast.Compare(
                    left=ast.Subscript(
                        ast.Name("row", ast.Load()),
                        ast.Constant(column),
                        ast.Load(),
                    ),
                    ops=[ast.Is()],
                    comparators=[ast.Constant(None)],
                )
                for column in columns
            ],
        )
    return ast.If(
        test=stmt,
        body=[ast.Return(ast.Constant(None))],
        orelse=[],
    )


def render_factory_call(ctx: Context, mapper: Mapper) -> ast.Assign:
    return ast.Assign(
        [ast.Name(mapper.name, ast.Store())],
        ast.Call(
            func=ast.Name(mapper.cls.__name__, ast.Load()),
            args=[],
            keywords=[
                ast.keyword(
                    field,
                    (
                        ast.Subscript(
                            ast.Name("row", ast.Load()),
                            ast.Constant(column[0]),
                            ast.Load(),
                        )
                    ),
                )
                for field, column in ctx.fields_to_columns[mapper].items()
            ],
        ),
    )


def render_mapper_call(mapper: Mapper) -> ast.expr:
    return ast.Call(
        func=ast.Name(mapper.func_name, ast.Load()),
        args=[ast.Name("row", ast.Load())],
        keywords=[],
    )


def render_rel_factory_call(
    ctx: Context,
    left: str,
    field: str,
    rel: Relationship,
    accessor: Accessor,
) -> Generator[ast.stmt, None, None]:
    right: str = rel.target_name.lower()

    if isinstance(rel, Assign):
        target = (
            ast.Attribute(
                ast.Name(ctx.mappers[left].name, ast.Load()),
                field,
                ast.Store(),
            )
            if accessor == "attr"
            else ast.Subscript(
                ast.Name(ctx.mappers[left].name, ast.Load()),
                ast.Constant(field),
                ast.Store(),
            )
        )
        yield ast.Assign([target], render_mapper_call(ctx.mappers[right]))

    elif isinstance(rel, Append):
        left_mapper = ctx.mappers[left]

        if accessor == "item":
            yield ast.If(
                test=ast.Compare(
                    left=ast.Constant(field),
                    ops=[ast.NotIn()],
                    comparators=[ast.Name(left_mapper.name, ast.Load())],
                ),
                body=[
                    ast.Assign(
                        [
                            ast.Subscript(
                                ast.Name(left_mapper.name, ast.Load()),
                                ast.Constant(field),
                                ast.Store(),
                            ),
                        ],
                        ast.List([], ast.Load()),
                    )
                ],
                orelse=[],
            )
            value = ast.Subscript(
                ast.Name(ctx.mappers[left].name, ast.Load()),
                ast.Constant(field),
                ast.Load(),
            )
        else:
            value = ast.Attribute(
                ast.Name(ctx.mappers[left].name, ast.Load()),
                field,
                ast.Load(),
            )

        yield ast.If(
            test=ast.NamedExpr(
                target=ast.Name("obj", ast.Store()),
                value=ast.Call(
                    func=ast.Name(ctx.mappers[right].func_name, ast.Load()),
                    args=[ast.Name("row", ast.Load())],
                    keywords=[],
                ),
            ),
            body=[
                ast.Expr(
                    ast.Call(
                        func=ast.Attribute(value, "append", ast.Load()),
                        args=[render_mapper_call(ctx.mappers[right])],
                        keywords=[],
                    )
                )
            ],
            orelse=[],
        )
    else:
        raise NotImplemented


def render_obj_id(ctx: Context, mapper: EntityMapper) -> ast.stmt:
    if len(mapper.id) == 1:
        id_field = mapper.id[0]
        id_col = ctx.fields_to_columns[mapper][id_field][0]
        id_val = ast.Subscript(
            ast.Name("row", ast.Load()),
            ast.Constant(id_col),
            ast.Load(),
        )
    else:
        id_val = ast.Tuple(
            [
                ast.Subscript(
                    ast.Name("row", ast.Load()),
                    ast.Constant(ctx.fields_to_columns[mapper][field][0]),
                    ast.Load(),
                )
                for field in mapper.id
            ],
            ast.Load(),
        )

    return ast.Assign([ast.Name(mapper.id_name, ast.Store())], id_val)


def render_get_or_create(
    ctx: Context, mapper: Mapper
) -> Generator[ast.stmt, None, None]:
    yield ast.Assign(
        [ast.Name(mapper.name, ast.Store())],
        ast.Call(
            func=ast.Attribute(
                ast.Name(mapper.id_map_name, ast.Load()), "get", ast.Load()
            ),
            args=[ast.Name(mapper.id_name, ast.Load())],
            keywords=[],
        ),
    )

    assign = render_factory_call(ctx, mapper)
    assign.targets.append(
        ast.Subscript(
            ast.Name(mapper.id_map_name, ast.Load()),
            ast.Name(mapper.id_name, ast.Load()),
            ast.Store(),
        ),
    )
    yield ast.If(
        test=ast.Compare(
            left=ast.Name(mapper.name, ast.Load()),
            ops=[ast.Is()],
            comparators=[ast.Constant(None)],
        ),
        body=[assign],
        orelse=[],
    )


def render_entity_mapper(ctx: Context, mapper: EntityMapper) -> ast.stmt:
    id_columns = [
        cols[0]
        for field, cols in ctx.fields_to_columns[mapper].items()
        if field in mapper.id
    ]
    body = [
        render_check_for_none(id_columns),
        render_obj_id(ctx, mapper),
        *render_get_or_create(ctx, mapper),
        *chain.from_iterable(
            render_rel_factory_call(
                ctx,
                mapper.name,
                field,
                rel,
                mapper.accessor,
            )
            for field, rel in ctx.rels[mapper.name].items()
        ),
        ast.Return(
            ast.Name(mapper.name, ast.Load()),
        ),
    ]
    return ast.FunctionDef(
        name=mapper.func_name,
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg="rows"),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=body,
        decorator_list=[],
    )


def render_value_mapper(
    ctx: Context,
    mapper: ValueMapper,
) -> ast.stmt:
    columns = [col[0] for col in ctx.fields_to_columns[mapper].values()]
    if_body = []
    if mapper.reduce_none:
        if_body.append(render_check_for_none(columns))
    if_body += [
        render_factory_call(ctx, mapper),
        *chain.from_iterable(
            render_rel_factory_call(
                ctx,
                mapper.name,
                field,
                rel,
                mapper.accessor,
            )
            for field, rel in ctx.rels[mapper.name].items()
        ),
        ast.Return(
            ast.Name(mapper.name, ast.Load()),
        ),
    ]
    return ast.FunctionDef(
        name=mapper.func_name,
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg="rows"),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=if_body,
        decorator_list=[],
    )


def render_mappers(ctx: Context) -> Generator[ast.stmt, None, None]:
    for mapper in ctx.mappers.values():
        if isinstance(mapper, EntityMapper):
            yield render_entity_mapper(ctx, mapper)
        elif isinstance(mapper, ValueMapper):
            yield render_value_mapper(ctx, mapper)
        else:
            raise NotImplemented


def render_cycle_body(ctx: Context) -> Generator[ast.stmt, None, None]:
    if ctx.result_is_unary:
        yield ast.Assign(
            [
                ast.Name("root", ast.Store()),
            ],
            render_mapper_call(
                ctx.result_mappers[0],
            ),
        )
        yield ast.If(
            test=ast.Compare(
                left=ast.Name("last_root", ast.Load()),
                ops=[ast.IsNot()],
                comparators=[ast.Name("root", ast.Load())],
            ),
            body=[
                ast.If(
                    test=ast.Compare(
                        left=ast.Name("last_root", ast.Load()),
                        ops=[ast.IsNot()],
                        comparators=[ast.Constant(None)],
                    ),
                    body=[ast.Expr(ast.Yield(ast.Name("last_root", ast.Load())))],
                    orelse=[],
                ),
                ast.Assign(
                    targets=[ast.Name("last_root", ast.Store())],
                    value=ast.Name("root", ast.Load()),
                ),
            ],
            orelse=[],
        )
    else:
        if len(ctx.mappers) == 1:
            yield_value = ast.Name(ctx.result_mappers[0].name, ast.Load())
        else:
            yield_value = ast.Tuple(
                [
                    ast.Call(
                        func=ast.Name(mapper.func_name, ast.Load()),
                        args=[ast.Name("row", ast.Load())],
                        keywords=[],
                    )
                    for mapper in ctx.mappers.values()
                ],
                ast.Load(),
            )
        yield ast.Expr(ast.Yield(yield_value))


def render_post_cycle(ctx: Context) -> Iterable[ast.stmt]:
    if ctx.result_is_unary:
        yield ast.If(
            test=ast.Compare(
                left=ast.Name("last_root", ast.Load()),
                ops=[ast.IsNot()],
                comparators=[ast.Constant(None)],
            ),
            body=[ast.Expr(ast.Yield(ast.Name("last_root", ast.Load())))],
            orelse=[],
        )


def render_mapper_func(ctx: Context) -> ast.stmt:
    return ast.FunctionDef(
        name="mapper_func",
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg="rows"),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=[
            *render_identity_maps(ctx),
            *render_mappers(ctx),
            *render_last_root(ctx),
            render_cycle(ctx),
            *(render_post_cycle(ctx)),
        ],
        decorator_list=[],
    )


def render_module(ctx: Context) -> ast.Module:
    func = render_mapper_func(ctx)
    return ast.fix_missing_locations(ast.Module(body=[func], type_ignores=[]))
