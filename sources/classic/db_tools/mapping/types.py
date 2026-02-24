from typing import Generator, Callable, TypeVar, Literal

from ..types import Row


Result = TypeVar('Result')
Mapper = Callable[[], Generator[Result, Row, None]]

Accessor = Literal['attr', 'item']
