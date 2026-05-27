from typing import Literal, TypeVar, Callable, Generator, Iterator

from ..types import Row


Accessor = Literal['attr', 'item']
Result = TypeVar('Result')
MapperFunc = Callable[[Iterator[Row]], Generator[Result, Row, None]]
