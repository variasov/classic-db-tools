import ast
from typing import Generator, Iterable, TypeAlias, Callable, TypeVar

from ..types import Row

from .params import Relationship
from .context import Context
from .render import render_module


Result = TypeVar('Result')
Mapper: TypeAlias = Callable[[], Generator[Result, Row, None]]


def compile_mapper(
    result: Result,
    relationships: Iterable[Relationship],
    columns: tuple[str, ...],
) -> Mapper[Result]:
    ctx = Context(result, relationships, columns)

    ast_module = render_module(ctx)
    code = compile(ast_module, '<string>', 'exec')
    namespace = {
        mapper.cls.__name__: mapper.cls
        for mapper in ctx.mappers.values()
    }
    exec(code, namespace)
    func = namespace['mapper_func']

    # Ради удобства отладки добавим код маппера
    func.sources = lambda: ast.unparse(ast_module)

    return func
