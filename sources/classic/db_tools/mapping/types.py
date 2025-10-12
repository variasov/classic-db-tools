from typing import Generator, TypeAlias, Callable, TypeVar

from ..types import Row


Result = TypeVar('Result')
Mapper: TypeAlias = Callable[[], Generator[Result, Row, None]]
