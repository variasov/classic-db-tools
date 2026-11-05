from typing import Any, NoReturn, Sequence

from .dbapi import CursorDescription, CursorParams, Row


class CursorOutsideScopeError(RuntimeError):
    """
    Попытка использовать результат execute()/executemany() вне
    engine.conn()/engine.transaction(): соединение уже возвращено в пул.
    """


_MESSAGE = (
    'Result of execute() outside of engine.conn()/engine.transaction() '
    'is not available: the connection has already been returned '
    'to the pool. Call execute() inside a scope '
    'or use one()/all()/scalar().'
)


def _fail() -> NoReturn:
    raise CursorOutsideScopeError(_MESSAGE)


class DetachedCursor:
    """
    Курсор, отвязанный от соединения.

    Возвращается Query.execute()/executemany(), если они вызваны вне
    engine.conn()/engine.transaction(). Доступен только rowcount,
    остальные операции вызывают CursorOutsideScopeError.
    """

    def __init__(self, rowcount: int) -> None:
        self._rowcount = rowcount

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def description(self) -> CursorDescription:
        _fail()

    def execute(
        self,
        operation: str,
        parameters: CursorParams = None,
    ) -> None:
        _fail()

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[CursorParams],
    ) -> None:
        _fail()

    def close(self) -> None:
        pass

    def fetchone(self) -> Row:
        _fail()

    def fetchmany(self, size: int | None = None) -> Sequence[Row]:
        _fail()

    def fetchall(self) -> Sequence[Row]:
        _fail()

    def __iter__(self) -> Any:
        _fail()

    def __getattr__(self, name: str) -> Any:
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError(name)
        _fail()
