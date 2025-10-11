import ast
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import (
    Any, Type, Hashable, Generator, Iterable,
    TypeAlias, Callable, Literal, TypedDict
)

from .find_root import find_root_in_dag


Key = str | Type[Any]
MapperCache = dict[Hashable, Any]


class Param:
    pass


class MappingParam(ABC, Param):
    name: str
    cls: Type[Any]
    id: str | Iterable[str]
    accessor_type: Literal['attr', 'item']

    __slots__ = ('name', 'cls', 'id_fields')

    @property
    def id_name(self) -> str:
        return self.name + '_id'

    @property
    def identity_map_name(self) -> str:
        return self.name + '_map'

    @property
    def last_obj_name(self) -> str:
        return f'last_{self.name}'

    @abstractmethod
    def __hash__(self) -> int:
        pass


class AsCls(MappingParam):
    fields_to_columns: dict[str, str]

    def __init__(
        self,
        cls: Type[Any],
        id: str | Iterable[str] = 'id',
    ):
        self.name = cls.__name__.lower()
        self.cls = cls
        self.id_fields = (id,) if isinstance(id, str) else tuple(id)
        if issubclass(cls, dict):
            self.accessor_type = 'item'
        else:
            self.accessor_type = 'attr'

    def __hash__(self):
        return hash((self.cls, self.name, self.id_fields))

    def __eq__(self, other: Any) -> bool:
        return (
            self.__class__ == other.__class__ and
            self.cls == other.cls and
            self.name == other.name and
            self.id_fields == other.id_fields
        )


class Relationship(Param):
    left: str
    field: str
    right: str

    __slots__ = ('left', 'field', 'right')

    def __init__(self, left: Key, field: str, right: Key) -> None:
        self.field = field

        if isinstance(left, str):
            self.left = left.lower()
        else:
            self.left = left.__name__.lower()

        if isinstance(left, str):
            self.right = right.lower()
        else:
            self.right = right.__name__.lower()

    def __eq__(self, other: Any) -> bool:
        return (
            self.__class__ == other.__class__ and
            self.left == other.left and
            self.field == other.field and
            self.right == other.right
        )

    def __hash__(self) -> int:
        return hash((
            self.__class__,
            self.left,
            self.field,
            self.right,
        ))


class OneToOne(Relationship):
    pass


class OneToMany(Relationship):
    pass


def create_line_counter():
    _lineno = 0
    def inc_and_return():
        nonlocal _lineno
        _lineno += 1
        return _lineno

    return inc_and_return


Mapper: TypeAlias = Callable[[], Generator[Any, Any, None]]


def compile_mapper(
    mapper_params: Iterable[Param],
    columns: tuple[str, ...],
) -> Mapper:
    mappers: dict[str, MappingParam] = {
        param.name: param
        for param in mapper_params
        if isinstance(param, MappingParam)
    }
    mappers_relations: dict[str, tuple[Relationship, ...]] = {
        mapper.name: tuple((
            param
            for param in mapper_params
            if isinstance(param, Relationship) and param.right == mapper.name
        ))
        for mapper in mappers.values()
    }
    mappers_field_columns = {}
    for mapper in mappers.values():
        mappers_field_columns[mapper] = field_to_columns =  {}
        for column in columns:
            cls_name, field_name = column.split('__')
            if cls_name == mapper.name:
                field_to_columns[field_name] = column

    adjacency_tree = defaultdict(set)
    for mapper_name, relations in mappers_relations.items():
        for relation in relations:
            adjacency_tree[
                mappers[relation.left]
            ].add(mappers[relation.right])

    root: MappingParam | None = find_root_in_dag(adjacency_tree)

    lineno = create_line_counter()

    def render_columns(col_offset: int) -> Iterable[ast.stmt]:
        nonlocal lineno
        for index, column in enumerate(columns):
            yield ast.Assign(
                targets=[
                    ast.Name(id=column, ctx=ast.Store())
                ],
                value=ast.Constant(value=index),
                lineno=lineno(),
                col_offset=col_offset,
            )

    def render_identity_maps(col_offset: int) -> Iterable[ast.stmt]:
        nonlocal lineno
        for mapper in mappers.values():
            yield ast.Assign(
                targets=[
                    ast.Name(id=mapper.identity_map_name, ctx=ast.Store())
                ],
                value=ast.Dict(keys=[], values=[]),
                lineno=lineno(),
                col_offset=col_offset,
            )

    def render_last_root(col_offset: int) -> Iterable[ast.stmt]:
        nonlocal lineno
        if root is None:
            return []
        return [ast.Assign(
                targets=[
                    ast.Name(id=root.last_obj_name, ctx=ast.Store())
                ],
                value=ast.Constant(value=None),
            lineno=lineno(),
            col_offset=col_offset,
        )]

    def render_cycle(col_offset: int) -> ast.stmt:
        nonlocal lineno
        return ast.While(
            test=ast.Constant(value=True),
            body=list(render_cycle_body(col_offset + 1)),
            orelse=[],
            lineno=lineno(),
            col_offset=col_offset,
        )

    def render_input(col_offset: int) -> ast.stmt:
        nonlocal lineno
        return ast.Assign(
            targets=[ast.Name(id='row', ctx=ast.Store())],
            value=ast.Yield(),
            lineno=lineno(),
            col_offset=col_offset,
        )

    def render_break(col_offset: int) -> ast.stmt:
        nonlocal lineno
        return ast.If(
            test=ast.Compare(
                left=ast.Name(id='row', ctx=ast.Load()),
                ops=[ast.Is()],
                comparators=[ast.Constant(value=None)],
                lineno=lineno(),
                col_offset=col_offset,
            ),
            body=[ast.Break()],
            orelse=[],
            lineno=lineno(),
            col_offset=col_offset,
        )

    def render_cycle_body(col_offset: int) -> Generator[ast.stmt, None, None]:
        nonlocal lineno

        yield render_input(col_offset)
        yield render_break(col_offset)

        for mapper, mapper_rels in zip(
            mappers.values(),
            mappers_relations.values(),
        ):

            assign_id_lineno = lineno()
            # id = row[0]
            yield ast.Assign(
                targets=[ast.Name(id=mapper.id_name, ctx=ast.Store())],
                value=ast.Subscript(
                    value=ast.Name(id='row', ctx=ast.Load()),
                    slice=ast.Name(
                        id=mappers_field_columns[mapper][mapper.id_fields[0]],
                        ctx=ast.Load(),
                    ),
                    ctx=ast.Load(),
                    lineno=lineno(),
                    col_offset=col_offset,
                )
                if len(mapper.id_fields) == 1
                else ast.Tuple(
                    elts=[
                        ast.Subscript(
                            value=ast.Name(id='row', ctx=ast.Load()),
                            slice=ast.Name(
                                id=mappers_field_columns[mapper][field],
                                ctx=ast.Load(),
                            ),
                            ctx=ast.Load(),
                            lineno=lineno(),
                            col_offset=col_offset,
                        )
                        for field in mapper.id_fields
                    ],
                    ctx=ast.Load(),
                    lineno=lineno(),
                    col_offset=col_offset,
                ),
                lineno=assign_id_lineno,
                col_offset=col_offset,
            )

            # obj = identity_map.get(id)
            search_obj_lineno = lineno()
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
            if_lineno = lineno()
            if_body_lineno = lineno()
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
                                lineno=lineno(),
                                col_offset=col_offset + 1,
                            )
                        ),
                        lineno=lineno(),
                        col_offset=col_offset + 1,
                    )
                    for field, column
                    in mappers_field_columns[mapper].items()
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

            lineno()

            if root is not None and root is mapper:
                if_body.append(
                    ast.If(
                        test=ast.Compare(
                            left=ast.Name(
                                id=root.last_obj_name,
                                ctx=ast.Load(),
                            ),
                            ops=[ast.IsNot()],
                            comparators=[ast.Constant(value=None)]),
                        body=[
                            ast.Expr(
                                value=ast.Yield(
                                    value=ast.Name(
                                        id=root.last_obj_name,
                                        ctx=ast.Load(),
                                    )
                                ),
                                lineno=lineno(),
                                col_offset=col_offset + 1,
                            ),
                        ],
                        orelse=[],
                        lineno=lineno(),
                        col_offset=col_offset + 1,
                    )
                )
                if_body.append(
                    ast.Assign(
                        targets=[
                            ast.Name(id=root.last_obj_name, ctx=ast.Store())
                        ],
                        value=ast.Name(id=mapper.name, ctx=ast.Load()),
                        lineno=lineno(),
                        col_offset=col_offset + 1,
                    )
                )

            for relationship in mapper_rels:
                if mapper.accessor_type == 'attr':
                    if isinstance(relationship, OneToOne):
                        if_body.append(
                            ast.Assign(
                                targets=[
                                    ast.Attribute(
                                        value=ast.Name(
                                            id=relationship.left,
                                            ctx=ast.Load(),
                                        ),
                                        attr=relationship.field,
                                        ctx=ast.Store(),
                                    ),
                                ],
                                value=ast.Name(
                                    id=relationship.right,
                                    ctx=ast.Load(),
                                ),
                                lineno=lineno(),
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
                                                id=relationship.left,
                                                ctx=ast.Load(),
                                            ),
                                            attr=relationship.field,
                                            ctx=ast.Load()
                                        ),
                                        attr='append',
                                        ctx=ast.Load()
                                    ),
                                    args=[ast.Name(
                                        id=relationship.right,
                                        ctx=ast.Load(),
                                    )],
                                    keywords=[],
                                ),
                                lineno=lineno(),
                                col_offset=col_offset,
                            )
                        )
                    else:
                        raise NotImplemented
                elif mapper.accessor_type == 'item':
                    if_body.append(
                        ast.If(
                            test=ast.Compare(
                                left=ast.Constant(value=relation.field),
                                ops=[ast.NotIn()],
                                comparators=[
                                    ast.Name(id=relation.left, ctx=ast.Load())
                                ],
                            ),
                            body=[
                                ast.Assign(
                                    targets=[
                                        ast.Subscript(
                                            value=ast.Name(
                                                id=relation.left,
                                                ctx=ast.Load(),
                                            ),
                                            slice=ast.Constant(
                                                value=relation.field,
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
                            lineno=lineno(),
                            col_offset=col_offset,
                        )
                    )
                    if isinstance(relationship, OneToOne):
                        if_body.append(
                            ast.Assign(
                                targets=[
                                    ast.Subscript(
                                        value=ast.Name(
                                            id=relationship.left,
                                            ctx=ast.Load(),
                                        ),
                                        slice=ast.Constant(
                                            value=relationship.field,
                                        ),
                                        ctx=ast.Store(),
                                    ),
                                ],
                                value=ast.Name(
                                    id=relationship.right,
                                    ctx=ast.Load(),
                                ),
                                lineno=lineno(),
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
                                                id=relationship.left,
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
                                        id=relationship.right,
                                        ctx=ast.Load(),
                                    )],
                                    keywords=[],
                                ),
                                lineno=lineno(),
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

        if root is None:
            if len(mappers) == 1:
                yield_value = ast.Name(
                    id=list(mappers.keys())[0],
                    ctx=ast.Load(),
                )
            else:
                yield_value = ast.Tuple(
                    elts=[
                        ast.Name(
                            id=mapper.name,
                            ctx=ast.Load(),
                        )
                        for mapper in mappers.values()
                    ],
                    ctx=ast.Load(),
                )
            yield ast.Expr(
                value=ast.Yield(
                    value=yield_value,
                ),
                lineno=lineno(),
                col_offset=col_offset,
            )


    def render_post_cycle(col_offset: int) -> Iterable[ast.stmt]:
        nonlocal lineno
        if root is not None:
            yield ast.Expr(
                value=ast.Yield(
                    value=ast.Name(
                        id=root.last_obj_name,
                        ctx=ast.Load(),
                    ),
                ),
                lineno=lineno(),
                col_offset=col_offset,
            )

    def render_mapper_func(col_offset: int) -> ast.stmt:
        nonlocal lineno
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
                *render_columns(col_offset),
                *render_identity_maps(col_offset),
                *render_last_root(col_offset),
                render_cycle(col_offset + 1),
                *(render_post_cycle(col_offset)),
            ],
            decorator_list=[],
            lineno=lineno(),
            col_offset=col_offset,
        )

    def render_module() -> ast.Module:
        nonlocal lineno
        lineno = create_line_counter()
        func = render_mapper_func(col_offset=0)
        return ast.fix_missing_locations(
            ast.Module(body=[func], type_ignores=[])
        )

    ast_module = render_module()
    code = compile(ast_module, '<string>', 'exec')
    namespace = {
        mapper.cls.__name__: mapper.cls
        for mapper in mappers.values()
    }
    exec(code, namespace)
    func = namespace['mapper_func']

    # Ради удобства отладки добавим код маппера
    func.sources = lambda: ast.unparse(ast_module)

    return func
