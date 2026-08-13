import logging
from typing import Optional, Sequence

from classic.db_tools.dbapi import Cursor, CursorParams

from ..template import Template


class StaticTemplate(Template):
    """
    Статический SQL-шаблон.

    Используется под капотом Query и MapperQuery, снаружи недоступен.
    """

    def __init__(
        self,
        logger: logging.Logger,
        filepath: Optional[str] = None,
        content: Optional[str] = None,
    ):
        self.logger = logger
        assert filepath is None or content is None
        if filepath:
            self.filepath = filepath
            with open(self.filepath, 'rt') as file:
                self.content = file.read()
        elif content:
            self.filepath = None
            self.content = content
        else:
            raise NotImplementedError

    def execute(
        self,
        cursor: Cursor,
        params: CursorParams,
    ) -> Cursor:
        self.logger.debug(self.content)
        cursor.execute(self.content, params)
        return cursor

    def executemany(
        self,
        cursor: Cursor,
        params: Sequence[CursorParams],
    ) -> Cursor:
        self.logger.debug(self.content)
        cursor.executemany(self.content, params)
        return cursor
