import ast
from collections import defaultdict
from itertools import chain
from typing import (
    Iterable, List, Tuple, Dict, Optional,
    Generator, Callable, TypeVar, Literal,
)

from .params import (
    Parameter, Relationship, Assign, Append, Add, Entity, Value, Mapper,
)
from ..types import Row


Accessor = Literal['attr', 'item']
Result = TypeVar('Result')
MapperFunc = Callable[[], Generator[Result, Row, None]]

# Aliases for readability
Prefix = str
Field = str
Column = str

# Tuple positions for fields_to_columns
index = 0
col = 1


class Context:
    mapper: Dict[Prefix, Parameter]
    rels: Dict[Prefix, Dict[Field, Relationship]]
    result: Tuple[Prefix, Parameter]
    columns: Optional[Tuple[Column, ...]]
    fields_to_columns: Dict[
        Parameter, Dict[Field, Tuple[int, Column]]
    ]

    def __init__(
        self,
        result: str,
        mapper: Mapper,
        columns: Tuple[Column, ...],
    ):
        self.mapper = {}
        self.rels = defaultdict(dict)
        self.result = (result, mapper.params[result])
        self.columns = None
        self.fields_to_columns = defaultdict(dict)
        self.parse_columns(columns, mapper)

    @staticmethod
    def accessor(param: Parameter) -> Accessor:
        return 'item' if issubclass(param.factory, dict) else 'attr'

    def namespace(self):
        return {
            mapper.factory.__name__: mapper.factory
            for mapper in self.mapper.values()
        }

    def column_for_field(self, param: Parameter, field: Field) -> str:
        try:
            return self.fields_to_columns[param][field][col]
        except KeyError:
            raise ValueError(f'For class {param} not found field {field}')

    def parse_columns(
        self,
        columns: Tuple[Column, ...],
        mapper: Mapper,
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

            param = mapper.params.get(prefix)
            if not param:
                raise ValueError(f'Prefix {prefix} not found in mapper')

            if param not in self.mapper:
                self.mapper[prefix] = param

                for prefix_, param_ in mapper.params.items():
                    for rel_field, rel in param_.relationships.items():
                        if (
                            rel.target == prefix and
                            rel not in self.rels[prefix]
                        ):
                            self.rels[prefix_][rel_field] = rel

            self.fields_to_columns[param][field_name] = index_, column


def compile_mapper_func(
    result: Result,
    mapper: Mapper,
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

    # For easy debugging add source code to func
    func.sources = lambda: ast.unparse(ast_module)

    return func


def render_identity_maps(ctx: Context) -> Iterable[ast.stmt]:
    for prefix in ctx.mapper:
        yield ast.Assign(
            [ast.Name(f'{prefix}_map', ast.Store())],
            ast.Dict(keys=[], values=[]),
        )


def render_last_root() -> ast.stmt:
    return ast.Assign(
        [ast.Name("last_root", ast.Store())],
        ast.Constant(None),
    )


def render_cycle(ctx: Context) -> ast.stmt:
    return ast.For(
        target=ast.Name("row", ast.Store()),
        iter=ast.Name("rows", ast.Load()),
        body=list(render_cycle_body(ctx)),
        orelse=[],
    )


def render_check_for_none(columns: List[int]) -> ast.stmt:
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
                            ast.Constant(column[index]),
                            ast.Load(),
                        )
                    ),
                )
                for field, column in ctx.fields_to_columns[param].items()
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
    left: Prefix,
    field: Field,
    rel: Relationship,
    accessor: Accessor,
) -> Generator[ast.stmt, None, None]:
    right: Prefix = rel.target

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
        yield ast.Assign([target], render_mapper_call(right))

    elif isinstance(rel, (Append, Add)):
        if accessor == "item":
            if isinstance(rel, Append):
                default  = ast.List([], ast.Load())
            elif isinstance(rel, Add):
                default = ast.Set([])
            else:
                raise NotImplemented

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

        yield ast.If(
            test=ast.NamedExpr(
                target=ast.Name("obj", ast.Store()),
                value=ast.Call(
                    func=ast.Name(f'map_{right}', ast.Load()),
                    args=[ast.Name("row", ast.Load())],
                    keywords=[],
                ),
            ),
            body=[
                ast.Expr(
                    ast.Call(
                        func=ast.Attribute(
                            value, rel.__class__.__name__.lower(), ast.Load(),
                        ),
                        args=[ast.Name('obj', ast.Load())],
                        keywords=[],
                    )
                )
            ],
            orelse=[],
        )
    else:
        raise NotImplemented


def render_obj_id(ctx: Context, prefix: Prefix, entity: Entity) -> ast.stmt:
    if len(entity.id) == 1:
        id_field = entity.id[0]
        id_col = ctx.fields_to_columns[entity][id_field][index]
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
                    ast.Constant(ctx.fields_to_columns[entity][field][index]),
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
        [ast.Name(prefix, ast.Store())],
        ast.Call(
            func=ast.Attribute(
                ast.Name(f'{prefix}_map', ast.Load()), "get", ast.Load()
            ),
            args=[ast.Name(f'{prefix}_id', ast.Load())],
            keywords=[],
        ),
    )

    assign = render_factory_call(ctx, prefix, param)
    assign.targets.append(
        ast.Subscript(
            ast.Name(f'{prefix}_map', ast.Load()),
            ast.Name(f'{prefix}_id', ast.Load()),
            ast.Store(),
        ),
    )
    yield ast.If(
        test=ast.Compare(
            left=ast.Name(prefix, ast.Load()),
            ops=[ast.Is()],
            comparators=[ast.Constant(None)],
        ),
        body=[assign],
        orelse=[],
    )


def render_entity(ctx: Context, prefix: Prefix, entity: Entity) -> ast.stmt:
    id_columns = [
        cols[index]
        for field, cols in ctx.fields_to_columns[entity].items()
        if field in entity.id
    ]
    body = [
        render_check_for_none(id_columns),
        render_obj_id(ctx, prefix, entity),
        *render_get_or_create(ctx, prefix, entity),
        *chain.from_iterable(
            render_rel_factory_call(
                prefix,
                field,
                rel,
                ctx.accessor(entity),
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
                ast.arg(arg="rows"),
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
    columns = [col_[index] for col_ in ctx.fields_to_columns[value].values()]
    if_body = []
    if value.reduce_none:
        if_body.append(render_check_for_none(columns))
    if_body += [
        render_factory_call(ctx, prefix, value),
        *chain.from_iterable(
            render_rel_factory_call(
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
    for prefix, mapper in ctx.mapper.items():
        if isinstance(mapper, Entity):
            yield render_entity(ctx, prefix, mapper)
        elif isinstance(mapper, Value):
            yield render_value(ctx, prefix, mapper)
        else:
            raise NotImplemented


def render_cycle_body(ctx: Context) -> Generator[ast.stmt, None, None]:
    yield ast.Assign(
        [ast.Name("root", ast.Store())],
        render_mapper_call(ctx.result[0]),
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


def render_post_cycle() -> ast.stmt:
    return ast.If(
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
            render_last_root(),
            render_cycle(ctx),
            render_post_cycle(),
        ],
        decorator_list=[],
    )


def render_module(ctx: Context) -> ast.Module:
    func = render_mapper_func(ctx)
    module = ast.Module(body=[func], type_ignores=[])
    return ast.fix_missing_locations(module)
