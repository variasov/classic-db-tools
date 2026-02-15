from typing import Generator, TypeAlias, Callable, TypeVar, Literal

from ..types import Row


Result = TypeVar('Result')
Mapper: TypeAlias = Callable[[], Generator[Result, Row, None]]

Accessor: TypeAlias = Literal['attr', 'item']
