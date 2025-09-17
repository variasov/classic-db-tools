from typing import Protocol, Sequence, Optional


class Cursor(Protocol):
    rowcount: int
    description: Sequence[tuple[str, int, int, int, int, bool]]

    def execute(
            self,
            operation: str,
            parameters: dict[str, object],
    ) -> None:
        pass

    def executemany(
            self,
            operation: str,
            seq_of_parameters: Sequence[dict[str, object]],
    ) -> None:
        pass

    def close(self) -> None:
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size: Optional[int]):
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
