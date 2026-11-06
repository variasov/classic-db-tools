from collections.abc import Generator, Iterator
from typing import Any, Literal, Protocol, TypeVar

Accessor = Literal["attr", "item"]
Result = TypeVar("Result")
Result_co = TypeVar("Result_co", covariant=True)


class MapperFunc(Protocol[Result_co]):
    """Скомпилированная функция-маппер строк результата в объекты."""

    def __call__(
        self,
        rows: Iterator[Any],
    ) -> Generator[Result_co, None, None]: ...

    def sources(self) -> str: ...
