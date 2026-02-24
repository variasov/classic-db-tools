from typing import (
    Any, Protocol, Sequence, Optional,
    Tuple, Dict, Union,
)


Row = Tuple[Any, ...]
CursorParams = Union[Dict[str, Any], Row]
CursorDescription = Sequence[Tuple[str, int, int, int, int, bool]]


class Cursor(Protocol):
    rowcount: int
    description: CursorDescription

    def execute(
            self,
            operation: str,
            parameters: CursorParams,
    ) -> None:
        pass

    def executemany(
            self,
            operation: str,
            seq_of_parameters: Sequence[CursorParams],
    ) -> None:
        pass

    def close(self) -> None:
        pass

    def fetchone(self) -> Row:
        pass

    def fetchmany(self, size: Optional[int]) -> Sequence[Row]:
        pass

    def fetchall(self):
        pass


class Connection(Protocol):
    autocommit: bool

    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def cursor(self) -> Cursor:
        pass
