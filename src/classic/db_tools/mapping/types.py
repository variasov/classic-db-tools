from collections.abc import Generator, Iterator
from typing import Literal, TypeVar

from classic.db_tools.dbapi import Row

Accessor = Literal["attr", "item"]
Result = TypeVar("Result", bound=type[object])


class MapperFunc:
    def __call__(
        self, rows: Iterator[Row]
    ) -> Generator[Result, None, None]: ...

    def sources(self) -> str: ...
