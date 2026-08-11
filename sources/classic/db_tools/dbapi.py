from typing import (
    Any, Protocol, Sequence, Optional,
    Tuple, Dict, Union,
)


Row = Tuple[Any, ...]
CursorParams = Union[Dict[str, Any], Row, None]
CursorDescription = Sequence[Tuple[str, int, int, int, int, bool]]


class Cursor(Protocol):
    rowcount: int
    description: CursorDescription

    def execute(
            self,
            operation: str,
            parameters: CursorParams = None,
    ) -> None:
        ...

    def executemany(
            self,
            operation: str,
            seq_of_parameters: Sequence[CursorParams],
    ) -> None:
        ...

    def close(self) -> None:
        ...

    def fetchone(self) -> Row:
        ...

    def fetchmany(self, size: Optional[int]) -> Sequence[Row]:
        ...

    def fetchall(self) -> Sequence[Row]:
        ...


class Connection(Protocol):
    autocommit: bool

    def close(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def cursor(self) -> Cursor:
        ...


class DBModule(Protocol):
    paramstyle: str

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        ...