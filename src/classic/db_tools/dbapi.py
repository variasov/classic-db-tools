from collections.abc import Sequence
from typing import (
    Any,
    Protocol,
)

Row = tuple[Any, ...]
CursorParams = dict[str, Any] | Row | None
CursorDescription = Sequence[tuple[str, int, int, int, int, bool]]


class Cursor(Protocol):
    rowcount: int
    description: CursorDescription

    def execute(
        self,
        operation: str,
        parameters: CursorParams = None,
    ) -> None: ...

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[CursorParams],
    ) -> None: ...

    def close(self) -> None: ...

    def fetchone(self) -> Row: ...

    def fetchmany(self, size: int | None) -> Sequence[Row]: ...

    def fetchall(self) -> Sequence[Row]: ...


class Connection(Protocol):
    autocommit: bool

    def close(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def cursor(self) -> Cursor: ...


class DBModule(Protocol):
    paramstyle: str

    def connect(self, *args: Any, **kwargs: Any) -> Any: ...
