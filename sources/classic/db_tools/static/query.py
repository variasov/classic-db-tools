from typing import Sequence

from classic.db_tools.types import Cursor, CursorParams


class StaticQuery:

    def __init__(
        self,
        filename: str = None,
        content: str = None,
    ):
        assert not filename or not content
        if filename:
            self.filename = filename
            with open(self.filename, 'rt') as file:
                self.content = file.read()
        elif content:
            self.filename = None
            self.content = content

    def execute(
        self,
        params: CursorParams = None,
        cursor: Cursor = None,
    ) -> Cursor:
        cursor.execute(self.content, params)
        return cursor

    def executemany(
        self,
        params: Sequence[CursorParams],
        cursor: Cursor = None,
    ) -> Cursor:
        cursor.executemany(self.content, params)
        return cursor
