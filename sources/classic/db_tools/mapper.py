import ast
from abc import ABC, abstractmethod
from typing import Any, Type, Hashable, TypedDict, Generator, Iterable

from classic.db_tools.types import Cursor


Key = str | Type[Any]
MapperCache = dict[Hashable, Any]


class Mapper(ABC):
    name: str
    cls: Type[Any]
    id_fields: str | Iterable[str]

    __slots__ = ('name', 'cls', 'id_fields')

    @property
    def id_name(self) -> str:
        return self.name + '_id'

    @property
    def dict_name(self) -> str:
        return self.name + '_map'

    @abstractmethod
    def __hash__(self) -> int:
        pass


class ToCls(Mapper):

    def __init__(
        self,
        cls: Type[Any],
        id: str | Iterable[str] = 'id',
    ):
        self.name = cls.__name__.lower()
        self.cls = cls
        self.id_fields = id

    def __hash__(self):
        return hash((self.cls, self.name, self.id_fields))

    def __eq__(self, other: Any) -> bool:
        return (
            self.__class__ == other.__class__ and
            self.cls == other.cls and
            self.name == other.name and
            self.id_fields == other.id_fields
        )


class ToDict(Mapper):

    def __init__(
        self,
        key: str | Type[TypedDict],
        id: str | Iterable[str] = 'id',
    ):
        if isinstance(key, str):
            self.cls = dict
        elif isinstance(key, type) and issubclass(key, TypedDict):
            self.cls = key
            key = key.__name__
        else:
            raise NotImplemented

        self.name = key.lower()
        self.id_fields = id

    def __hash__(self):
        return hash((self.cls, self.name, self.id_fields))

    def __eq__(self, other: Any) -> bool:
        return (
            self.__class__ == other.__class__ and
            self.cls == other.cls and
            self.name == other.name and
            self.id_fields == other.id_fields
        )


class ToNamedTuple(Mapper):
    pass


class Relationship:
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


def automap(cls):
    raise NotImplemented


def create_line_counter():
    _lineno = 0
    def inc_and_return():
        nonlocal _lineno
        _lineno += 1
        return _lineno

    return inc_and_return


def compile_mapper(params, returns, cursor: Cursor):
    mappers: dict[str, Mapper] = {
        param.name: param
        for param in params
        if isinstance(param, Mapper)
    }
    mappers_field_col_maps: dict[str, dict[str, int]] = {
        mapper.name: {}
        for mapper in mappers.values()
    }
    mappers_relations: dict[str, tuple[Relationship, ...]] = {
        mapper.name: tuple((
            param
            for param in params
            if isinstance(param, Relationship) and param.right == mapper.name
        ))
        for mapper in mappers.values()
    }
    for index, column_desc in enumerate(cursor.description):
        try:
            prefix, field_name = column_desc[0].split('__')
        except ValueError:
            continue

        fields_to_columns = mappers_field_col_maps.get(prefix)
        if fields_to_columns is not None:
            fields_to_columns[field_name] = index

    mappers_id_columns = {}
    for mapper in mappers.values():
        if isinstance(mapper.id_fields, str):
            try:
                mappers_id_columns[mapper.id_name] = (
                    mappers_field_col_maps[mapper.name][mapper.id_fields]
                )
            except KeyError as exc:
                raise Exception(
                    'Похоже, что в запросе не указаны алиасы '
                    'для получаемых полей'
                ) from exc
        else:
            mappers_id_columns[mapper.id_name] = tuple(
                mappers_field_col_maps[mapper.name][id]
                for id in mapper.id_fields
            )

    lineno = create_line_counter()

    def render_identity_maps(col_offset: int) -> Generator[ast.stmt, None, None]:
        nonlocal lineno
        for mapper in mappers.values():
            yield ast.Assign(
                targets=[
                    ast.Name(id=mapper.dict_name, ctx=ast.Store())
                ],
                value=ast.Dict(keys=[], values=[]),
                lineno=lineno(),
                col_offset=col_offset,
            )

    def render_cycle(col_offset: int) -> ast.stmt:
        nonlocal lineno
        body_lineno = lineno()
        return ast.For(
            target=ast.Name(id='row', ctx=ast.Store()),
            iter=ast.Name(id='rows', ctx=ast.Load()),
            body=list(render_cycle_body(col_offset + 1)),
            orelse=[],
            lineno=body_lineno,
            col_offset=col_offset,
        )

    def render_cycle_body(col_offset: int) -> Generator[ast.stmt, None, None]:
        nonlocal lineno

        for mapper, mapper_id_columns, mapper_rels in zip(
            mappers.values(),
            mappers_id_columns.values(),
            mappers_relations.values(),
        ):

            assign_id_lineno = lineno()
            if isinstance(mapper.id_fields, str):
                # id = row[0]
                yield ast.Assign(
                    targets=[ast.Name(id=mapper.id_name, ctx=ast.Store())],
                    value=ast.Subscript(
                        value=ast.Name(id='row', ctx=ast.Load()),
                        slice=ast.Constant(value=mapper_id_columns),
                        ctx=ast.Load(),
                        lineno=lineno(),
                        col_offset=col_offset,
                    ),
                    lineno=assign_id_lineno,
                    col_offset=col_offset,
                )
            else:
                # id = (row[0], row[1])
                yield ast.Assign(
                    targets=[ast.Name(id=mapper.id_name, ctx=ast.Store())],
                    value=ast.Tuple(
                        elts=[
                            ast.Subscript(
                                value=ast.Name(id='row', ctx=ast.Load()),
                                slice=ast.Constant(value=column),
                                ctx=ast.Load(),
                                lineno=lineno(),
                                col_offset=col_offset,
                            )
                            for column in mapper_id_columns
                        ],
                        ctx=ast.Load(),
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
                        value=ast.Name(id=mapper.dict_name, ctx=ast.Load()),
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
            #         field2=row[2],
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
                        value=ast.Subscript(
                            value=ast.Name(id='row', ctx=ast.Load()),
                            slice=ast.Constant(value=column),
                            ctx=ast.Load(),
                        ),
                        lineno=lineno(),
                        col_offset=col_offset + 1,
                    )
                    for field, column
                    in mappers_field_col_maps[mapper.name].items()
                ],
                lineno=if_body_lineno,
                col_offset=col_offset,
            )
            assign_obj = ast.Assign(
                targets=[
                    ast.Name(id=mapper.name, ctx=ast.Store()),
                    ast.Subscript(
                        value=ast.Name(id=mapper.dict_name, ctx=ast.Load()),
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
            for relationship in mapper_rels:
                if isinstance(relationship, OneToOne):
                    assign_one_to_one = ast.Assign(
                        targets=[
                            ast.Attribute(
                                value=ast.Name(id=relationship.left, ctx=ast.Load()),
                                attr=relationship.field,
                                ctx=ast.Store(),
                            ),
                        ],
                        value=ast.Name(id=relationship.right, ctx=ast.Load()),
                        lineno=lineno(),
                        col_offset=col_offset,
                    )
                    if_body.append(assign_one_to_one)
                elif isinstance(relationship, OneToMany):
                    assign_one_to_many = ast.Expr(
                        value=ast.Call(
                            func=ast.Attribute(
                                value=ast.Attribute(
                                    value=ast.Name(id=relationship.left, ctx=ast.Load()),
                                    attr=relationship.field,
                                    ctx=ast.Load()
                                ),
                                attr='append',
                                ctx=ast.Load()
                            ),
                            args=[ast.Name(id=relationship.right, ctx=ast.Load())],
                            keywords=[],
                        ),
                        lineno=lineno(),
                        col_offset=col_offset,
                    )
                    if_body.append(assign_one_to_many)

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

    def render_return(col_offset: int) -> ast.stmt:
        nonlocal lineno
        if isinstance(returns, str):
            mapper = mappers[returns.lower()]
        elif isinstance(returns, type):
            mapper = mappers[returns.__name__.lower()]
        else:
            raise NotImplemented
        return ast.Return(
            value=ast.Name(id=mapper.dict_name, ctx=ast.Load()),
            lineno=lineno(),
            col_offset=col_offset,
        )

    def render_mapper_func(col_offset: int) -> ast.stmt:
        nonlocal lineno
        func_lineno = lineno()
        return ast.FunctionDef(
            name='mapper_func',
            args=ast.arguments(
                posonlyargs=[], args=[ast.arg(arg='rows')], kwonlyargs=[],
                kw_defaults=[], defaults=[],
            ),
            body=[
                *render_identity_maps(col_offset + 1),
                render_cycle(col_offset + 1),
                render_return(col_offset + 1),
            ],
            decorator_list=[],
            lineno=func_lineno,
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
    func.__generated__ = lambda: ast.unparse(ast_module)

    return func
