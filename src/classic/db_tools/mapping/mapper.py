import ast
from collections import defaultdict
from itertools import chain
from typing import (
    Any, Iterable, List, Tuple, Dict,
    Optional, Generator, NamedTuple, Type, cast,
)
import threading

from classic.db_tools.dbapi import Cursor

from .params import (
    Parameter, Relationship, Assign, Append, Add,
    Entity, Value, Mapping,
)

from .types import Accessor, MapperFunc


# Aliases for readability
Prefix = str
Field = str
Column = str

# Tuple positions for fields_to_columns
INDEX = 0
COLUMN = 1


class Mapper:

    def __init__(self, mapping: Mapping):
        self._cache = {}
        self._lock = threading.RLock()
        self._mapping = mapping
        self._compile_mapper_func = compile_mapper_func

    def func_for_cursor(
        self,
        cursor: Cursor,
        mapping: Mapping,
        result: str,
    ) -> MapperFunc:
        columns = tuple(column[0] for column in cursor.description)
        key = (result, mapping or self._mapping, columns)

        with self._lock:
            mapper = self._cache.get(key)
            if not mapper:
                mapper = self._compile_mapper_func(*key)
                self._cache[key] = mapper

        return mapper


class ResultParam(NamedTuple):
    prefix: Prefix
    param: Parameter


class Context:
    mapper: Dict[Prefix, Parameter]
    rels: Dict[Prefix, Dict[Field, Relationship]]
    result: ResultParam
    columns: Optional[Tuple[Column, ...]]
    fields_to_columns: Dict[
        Prefix, Dict[Field, Tuple[int, Column]]
    ]

    def __init__(
        self,
        result: str,
        mapping: Mapping,
        columns: Tuple[Column, ...],
    ):
        self.mapper = {}
        self.rels = defaultdict(dict)
        self.result = ResultParam(result, mapping[result])
        self.columns = None
        self.fields_to_columns = defaultdict(dict)
        self.parse_columns(columns, mapping)

    @staticmethod
    def accessor(param: Parameter) -> Accessor:
        try:
            return 'item' if issubclass(
                cast(Type[Any], param.factory), dict
            ) else 'attr'
        except TypeError:
            return 'item'

    def namespace(self):
        return {
            mapper.factory.__name__: mapper.factory
            for mapper in self.mapper.values()
        }

    def parse_columns(
        self,
        columns: Tuple[Column, ...],
        mapping: Mapping,
    ):
        self.columns = columns
        for index_, column in enumerate(columns):
            try:
                prefix, field_name = column.lower().split('__')
            except ValueError as e:
                raise ValueError(
                    f'Column {column} are not contains name of cls '
                    f'and name of field, concatenated with __'
                ) from e

            param = mapping.get(prefix)
            if not param:
                raise ValueError(f'Prefix {prefix} not found in mapper')

            if param not in self.mapper:
                self.mapper[prefix] = param

                for prefix_, param_ in mapping.items():
                    for rel_field, rel in param_.relationships.items():
                        if (
                            rel.target == prefix and
                            rel not in self.rels[prefix]
                        ):
                            self.rels[prefix_][rel_field] = rel

            self.fields_to_columns[prefix][field_name] = index_, column


def compile_mapper_func(
    result: str,
    mapper: Mapping,
    columns: Tuple[Column, ...],
) -> MapperFunc:
    ctx = Context(result, mapper, columns)

    # Mapping class_name: Class
    namespace = ctx.namespace()

    # AST Tree with module
    ast_module = render_module(ctx)

    # Compile func
    code = compile(ast_module, '<string>', 'exec')
    exec(code, namespace)

    func = namespace['mapper_func']

    # Add source code to func for easy debugging
    setattr(func, 'sources', lambda: ast.unparse(ast_module))

    return cast(MapperFunc, func)


def render_identity_maps(ctx: Context) -> Iterable[ast.stmt]:
    for prefix, mapper in ctx.mapper.items():
        if isinstance(mapper, Entity):
            yield ast.Assign(
                [ast.Name(f'{prefix}_map', ast.Store())],
                ast.Dict(keys=[], values=[]),
            )


def render_last_obj() -> ast.stmt:
    return ast.Assign(
        [ast.Name("last_obj", ast.Store())],
        ast.Constant(None),
    )


def render_cycle(ctx: Context) -> ast.stmt:
    if isinstance(ctx.result.param, Entity):
        assign_stmt = ast.Assign(
            [
                ast.Tuple([
                    ast.Name("obj", ast.Store()),
                    ast.Name("_", ast.Store()),
                ], ast.Store()),
            ],
            ast.Call(
                func=ast.Name(f'map_{ctx.result.prefix}', ast.Load()),
                args=[ast.Name("row_", ast.Load())],
                keywords=[],
            )
        )
    elif isinstance(ctx.result.param, Value):
        assign_stmt = ast.Assign(
            [ast.Name("obj", ast.Store())],
            ast.Call(
                func=ast.Name(f'map_{ctx.result.prefix}', ast.Load()),
                args=[ast.Name("row_", ast.Load())],
                keywords=[],
            )
        )
    else:
        raise NotImplementedError

    return ast.For(
        target=ast.Name("row_", ast.Store()),
        iter=ast.Name("rows", ast.Load()),
        body=[
            assign_stmt,
            ast.If(
                test=ast.Compare(
                    left=ast.Name("last_obj", ast.Load()),
                    ops=[ast.IsNot()],
                    comparators=[ast.Name("obj", ast.Load())],
                ),
                body=[
                    ast.If(
                        test=ast.Compare(
                            left=ast.Name("last_obj", ast.Load()),
                            ops=[ast.IsNot()],
                            comparators=[ast.Constant(None)],
                        ),
                        body=[
                            ast.Expr(
                                ast.Yield(ast.Name("last_obj", ast.Load()))
                            ),
                        ],
                        orelse=[],
                    ),
                    ast.Assign(
                        [ast.Name("last_obj", ast.Store())],
                        ast.Name("obj", ast.Load()),
                    ),
                ],
                orelse=[],
            ),
        ],
        orelse=[],
    )


def render_check_for_none(columns: List[int]) -> ast.expr:
    if len(columns) == 1:
        return ast.Compare(
            left=ast.Subscript(
                ast.Name("row", ast.Load()),
                ast.Constant(columns[0]),
                ast.Load(),
            ),
            ops=[ast.Is()],
            comparators=[ast.Constant(None)],
        )
    else:
        return ast.BoolOp(
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


def render_factory_call(
    ctx: Context, prefix: Prefix, param: Parameter,
) -> ast.Assign:
    return ast.Assign(
        [ast.Name(prefix, ast.Store())],
        ast.Call(
            func=ast.Name(param.factory.__name__, ast.Load()),
            args=[],
            keywords=[
                ast.keyword(
                    field,
                    (
                        ast.Subscript(
                            ast.Name("row", ast.Load()),
                            ast.Constant(column[INDEX]),
                            ast.Load(),
                        )
                    ),
                )
                for field, column in ctx.fields_to_columns[prefix].items()
            ],
        ),
    )


def render_mapper_call(prefix: Prefix) -> ast.expr:
    return ast.Call(
        func=ast.Name(f'map_{prefix}', ast.Load()),
        args=[ast.Name("row", ast.Load())],
        keywords=[],
    )


def render_rel_factory_call(
    ctx: Context,
    left: Prefix,
    field: Field,
    rel: Relationship,
    accessor: Accessor,
) -> Generator[ast.stmt, None, None]:
    right: Prefix = rel.target
    right_mapper = ctx.mapper[right]

    if isinstance(rel, Assign):
        target = (
            ast.Attribute(
                ast.Name(left, ast.Load()),
                field,
                ast.Store(),
            )
            if accessor == "attr"
            else ast.Subscript(
                ast.Name(left, ast.Load()),
                ast.Constant(field),
                ast.Store(),
            )
        )
        if isinstance(right_mapper, Entity):
            yield ast.Assign(
                [
                    ast.Tuple([
                        target,
                        ast.Name('_', ast.Store())
                    ], ast.Store()),
                ],
                render_mapper_call(right),
            )
        elif isinstance(right_mapper, Value):
            yield ast.Assign(
                [target],
                render_mapper_call(right),
            )
        else:
            raise NotImplementedError

    elif isinstance(rel, (Append, Add)):
        if accessor == "item":
            if isinstance(rel, Append):
                default = ast.List([], ast.Load())
            elif isinstance(rel, Add):
                default = ast.Set([])

            yield ast.If(
                test=ast.Compare(
                    left=ast.Constant(field),
                    ops=[ast.NotIn()],
                    comparators=[ast.Name(left, ast.Load())],
                ),
                body=[
                    ast.Assign(
                        [
                            ast.Subscript(
                                ast.Name(left, ast.Load()),
                                ast.Constant(field),
                                ast.Store(),
                            ),
                        ],
                        default,
                    )
                ],
                orelse=[],
            )
            value = ast.Subscript(
                ast.Name(left, ast.Load()),
                ast.Constant(field),
                ast.Load(),
            )
        else:
            value = ast.Attribute(
                ast.Name(left, ast.Load()), field, ast.Load(),
            )

        if isinstance(right_mapper, Entity):
            yield ast.Assign(
                [
                    ast.Tuple([
                        ast.Name(right, ast.Store()),
                        ast.Name(f'{right}_id', ast.Store()),
                    ], ast.Store()),
                ],
                ast.Call(
                    func=ast.Name(f'map_{right}', ast.Load()),
                    args=[ast.Name("row", ast.Load())],
                    keywords=[],
                )
            )
            yield ast.If(
                test=ast.BoolOp(
                    ast.And(),
                    [
                        ast.Compare(
                            ast.Name(right, ast.Load()),
                            [ast.IsNot()],
                            [ast.Constant(None)],
                        ),
                        ast.Compare(
                            ast.Name(f'{right}_id', ast.Load()),
                            [ast.NotIn()],
                            [ast.Name(f'{left}_{field}', ast.Load())],
                        )
                    ]
                ),
                body=[
                    ast.Expr(
                        ast.Call(
                            func=ast.Attribute(
                                ast.Name(
                                    f'{left}_{field}', ast.Load(),
                                ), "add", ast.Load(),
                            ),
                            args=[ast.Name(f'{right}_id', ast.Load())],
                            keywords=[],
                        )
                    ),
                    ast.Expr(
                        ast.Call(
                            func=ast.Attribute(
                                value, rel.__class__.__name__.lower(), ast.Load(),
                            ),
                            args=[ast.Name(right, ast.Load())],
                            keywords=[],
                        )
                    )
                ],
                orelse=[],
            )
        elif isinstance(right_mapper, Value):
            yield ast.Assign(
                [ast.Name(right, ast.Store())],
                ast.Call(
                    func=ast.Name(f'map_{right}', ast.Load()),
                    args=[ast.Name("row", ast.Load())],
                    keywords=[],
                )
            )
            yield ast.If(
                test=ast.Compare(
                    ast.Name(right, ast.Load()),
                    [ast.IsNot()], [ast.Constant(None)],
                ),
                body=[
                    ast.Expr(
                        ast.Call(
                            func=ast.Attribute(
                                value, rel.__class__.__name__.lower(), ast.Load(),
                            ),
                            args=[ast.Name(right, ast.Load())],
                            keywords=[],
                        )
                    )
                ],
                orelse=[],
            )
        else:
            raise NotImplementedError
    else:
        raise NotImplementedError


def render_obj_id(ctx: Context, prefix: Prefix, entity: Entity) -> ast.stmt:
    if len(entity.id) == 1:
        id_field = entity.id[0]
        id_col = ctx.fields_to_columns[prefix][id_field][INDEX]
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
                    ast.Constant(ctx.fields_to_columns[prefix][field][INDEX]),
                    ast.Load(),
                )
                for field in entity.id
            ],
            ast.Load(),
        )

    return ast.Assign([ast.Name(f'{prefix}_id', ast.Store())], id_val)


def render_get_or_create(
    ctx: Context, prefix: Prefix, param: Parameter,
) -> Generator[ast.stmt, None, None]:
    yield ast.Assign(
        [ast.Name("obj_with_rels", ast.Store())],
        ast.Call(
            func=ast.Attribute(
                ast.Name(f'{prefix}_map', ast.Load()), "get", ast.Load()
            ),
            args=[ast.Name(f'{prefix}_id', ast.Load())],
            keywords=[],
        ),
    )

    rel_names = [
        f'{prefix}_{field}'
        for field, rel in ctx.rels[prefix].items()
        if isinstance(rel, (Append, Add))
    ]
    if_body: List[ast.stmt] = [
        render_factory_call(ctx, prefix, param),
        *(
            ast.Assign(
                [ast.Name(name, ast.Store())],
                ast.Call(ast.Name("set", ast.Load()), [], [])
            ) for name in rel_names
        ),
        ast.Assign(
            [ast.Subscript(
                ast.Name(f'{prefix}_map', ast.Load()),
                ast.Name(f'{prefix}_id', ast.Load()),
                ast.Store(),
            )],
            ast.Tuple([
                ast.Name(prefix, ast.Load()),
                *(
                    ast.Name(name, ast.Load())
                    for name in rel_names
                )
            ], ast.Load()),
        )
    ]
    else_body: List[ast.stmt] = [
        ast.Assign(
            [
                ast.Tuple([
                    ast.Name(prefix, ast.Store()),
                    *(
                        ast.Name(name, ast.Store())
                        for name in rel_names
                    ),
                ], ast.Store()),
            ],
            ast.Name("obj_with_rels", ast.Load()),
        ),
    ]

    yield ast.If(
        test=ast.Compare(
            left=ast.Name("obj_with_rels", ast.Load()),
            ops=[ast.Is()],
            comparators=[ast.Constant(None)],
        ),
        body=if_body,
        orelse=else_body,
    )


def render_entity(ctx: Context, prefix: Prefix, entity: Entity) -> ast.stmt:
    id_columns = [
        cols[INDEX]
        for field, cols in ctx.fields_to_columns[prefix].items()
        if field in entity.id
    ]
    body = [
        ast.If(
            test=render_check_for_none(id_columns),
            body=[
                ast.Return(
                    ast.Tuple([
                        ast.Constant(None),
                        ast.Constant(None),
                    ], ast.Load()),
                ),
            ],
            orelse=[],
        ),
        render_obj_id(ctx, prefix, entity),
        *render_get_or_create(ctx, prefix, entity),
        *chain.from_iterable(
            render_rel_factory_call(
                ctx,
                prefix,
                field,
                rel,
                ctx.accessor(entity),
            )
            for field, rel in ctx.rels[prefix].items()
        ),
        ast.Return(
            ast.Tuple([
                ast.Name(prefix, ast.Load()),
                ast.Name(f'{prefix}_id', ast.Load()),
            ], ast.Load()),
        ),
    ]
    return ast.FunctionDef(
        name=f'map_{prefix}',
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg="row"),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=body,
        decorator_list=[],
    )


def render_value(
    ctx: Context,
    prefix: Prefix,
    value: Value,
) -> ast.stmt:
    columns = [
        col_[INDEX] for col_
        in ctx.fields_to_columns[prefix].values()
    ]
    if_body = []
    if value.reduce_none:
        if_body.append(
            ast.If(
                test=render_check_for_none(columns),
                body=[
                    ast.Return(ast.Constant(None))
                ],
                orelse=[],
            ),
        )
    if_body += [
        render_factory_call(ctx, prefix, value),
        *chain.from_iterable(
            render_rel_factory_call(
                ctx,
                prefix,
                field,
                rel,
                ctx.accessor(value),
            )
            for field, rel in ctx.rels[prefix].items()
        ),
        ast.Return(
            ast.Name(prefix, ast.Load()),
        ),
    ]
    return ast.FunctionDef(
        name=f'map_{prefix}',
        args=ast.arguments(
            posonlyargs=[],
            args=[
                ast.arg(arg="row"),
            ],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=if_body,
        decorator_list=[],
    )


def render_mappers(ctx: Context) -> Generator[ast.stmt, None, None]:
    for prefix, mapper in ctx.mapper.items():
        if isinstance(mapper, Entity):
            yield render_entity(ctx, prefix, mapper)
        elif isinstance(mapper, Value):
            yield render_value(ctx, prefix, mapper)
        else:
            raise NotImplementedError


def render_return() -> ast.stmt:
    return ast.If(
        test=ast.Compare(
            left=ast.Name("last_obj", ast.Load()),
            ops=[ast.IsNot()],
            comparators=[ast.Constant(None)],
        ),
        body=[ast.Expr(ast.Yield(ast.Name("last_obj", ast.Load())))],
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
            render_last_obj(),
            render_cycle(ctx),
            render_return(),
        ],
        decorator_list=[],
    )


def render_module(ctx: Context) -> ast.Module:
    func = render_mapper_func(ctx)
    module = ast.Module(body=[func], type_ignores=[])
    return ast.fix_missing_locations(module)
