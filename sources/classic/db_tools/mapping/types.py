from typing import (
    Generator, Callable, TypeVar,
    Literal, Union, Tuple, Type, Any
)

from ..types import Row


Result = TypeVar('Result')
MapperFunc = Callable[[], Generator[Result, Row, None]]

Accessor = Literal['attr', 'item']

Class = Type[Any]
T = TypeVar('T')
Target = Union[T, 'str']
ID = Union[str, Tuple[str, ...]]
