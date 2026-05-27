from abc import ABC, abstractmethod
from typing import Sequence

from classic.db_tools.types import Cursor, CursorParams


class Template(ABC):

    @abstractmethod
    def execute(
        self,
        cursor: Cursor,
        params: CursorParams,
    ) -> Cursor:
        pass

    @abstractmethod
    def executemany(
        self,
        cursor: Cursor,
        params: Sequence[CursorParams],
    ) -> Cursor:
        pass
