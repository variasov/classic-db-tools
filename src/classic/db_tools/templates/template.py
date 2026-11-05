from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from classic.db_tools.dbapi import Cursor


class Template(ABC):
    """
    Интерфейс SQL-шаблона.
    """

    @abstractmethod
    def execute(
        self,
        cursor: Cursor,
        params: dict[str, Any],
    ) -> Cursor:
        pass

    @abstractmethod
    def executemany(
        self,
        cursor: Cursor,
        params: Sequence[dict[str, Any]],
    ) -> Cursor:
        pass
