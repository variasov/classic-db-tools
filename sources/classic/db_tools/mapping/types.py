from typing import (
    Any, Type, Hashable, Generator,
    TypeAlias, Callable, TypeVar,
)

from ..types import Row


Key = str | Type[Any]
MapperCache = dict[Hashable, Any]

Result = TypeVar('Result')
Mapper: TypeAlias = Callable[[], Generator[Result, Row, None]]
