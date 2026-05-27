from abc import ABC, abstractmethod
from typing import Any, Dict, Sequence

from classic.db_tools.types import Cursor


class Template(ABC):

    @abstractmethod
    def execute(
        self,
        cursor: Cursor,
        params: Dict[str, Any],
    ) -> Cursor:
        pass

    @abstractmethod
    def executemany(
        self,
        cursor: Cursor,
        params: Sequence[Dict[str, Any]],
    ) -> Cursor:
        pass
