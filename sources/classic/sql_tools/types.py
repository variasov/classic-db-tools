from typing import Protocol, Sequence, Optional


class Cursor(Protocol):
    rowcount: int

    def execute(self, operation: str, parameters: dict[str, object]) -> None:
        pass

    def executemany(self, operation: str, seq_of_parameters: Sequence[dict[str, object]]) -> None:
        pass

    def close(self):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size: Optional[int]):
        pass

    def fetchall(self):
        pass


class Connection(Protocol):

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self) -> Cursor:
        pass
