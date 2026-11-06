from collections.abc import Sequence
from typing import (
    Any,
    Protocol,
    TypeVar,
)

Row = tuple[Any, ...]
CursorParams = dict[str, Any] | Sequence[Any] | None
CursorDescription = Sequence[Sequence[Any]]

# Тип строки результата. Выводится из возвращаемого типа фабрики соединений.
RowT = TypeVar("RowT")
RowT_co = TypeVar("RowT_co", covariant=True)


class Cursor(Protocol[RowT_co]):
    """Минимальный протокол курсора DB API 2.0, нужный библиотеке."""

    @property
    def rowcount(self) -> int: ...

    @property
    def description(self) -> Any: ...

    # Типы параметров намеренно Any: драйверы принимают разные подмножества
    # (контравариантность), а библиотека передаёт dict/list.
    def execute(
        self,
        operation: str,
        parameters: Any = ...,
        /,
    ) -> Any: ...

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Any,
        /,
    ) -> Any: ...

    def close(self) -> Any: ...

    def fetchone(self) -> RowT_co | None: ...

    def fetchmany(self, size: int, /) -> Sequence[RowT_co]: ...

    def fetchall(self) -> Sequence[RowT_co]: ...


class Connection(Protocol[RowT_co]):
    """Минимальный протокол соединения DB API 2.0, нужный библиотеке."""

    def close(self) -> Any: ...

    def commit(self) -> Any: ...

    def rollback(self) -> Any: ...

    def cursor(self) -> Cursor[RowT_co]: ...


class DBModule(Protocol):
    """Модуль драйвера БД."""

    @property
    def paramstyle(self) -> str: ...

    def connect(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
