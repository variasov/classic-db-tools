from typing import Literal, TypeVar, Generator, Iterator, Type

from ..types import Row


Accessor = Literal['attr', 'item']
Result = TypeVar('Result', bound=Type[object])


class MapperFunc:

    def __call__(
        self, rows: Iterator[Row]
    ) -> Generator[Result, None, None]:
        ...

    def sources(self) -> str:
        ...
