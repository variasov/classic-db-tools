import ast
from typing import Dict, Tuple

from .context import Context
from .render import render_module
from .mappers import Mapper
from .types import MapperFunc, Result


def compile_mapper_func(
    result: Result,
    mappers: Dict[str, Mapper],
    columns: Tuple[str, ...],
) -> MapperFunc:
    ctx = Context(result, mappers, columns)

    ast_module = render_module(ctx)
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
