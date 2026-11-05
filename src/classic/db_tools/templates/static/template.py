import logging
from collections.abc import Sequence
from typing import Any

from classic.db_tools.dbapi import Cursor, CursorParams
from classic.db_tools.templates.template import Template


class StaticTemplate(Template):
    """
    Статический SQL-шаблон.

    Используется под капотом Query и MapperQuery, снаружи недоступен.
    """

    def __init__(
        self,
        logger: logging.Logger,
        filepath: str | None = None,
        content: str | None = None,
    ) -> None:
        self.logger = logger
        assert filepath is None or content is None
        self.filepath: str | None
        if filepath:
            self.filepath = filepath
            with open(self.filepath, encoding="utf-8") as file:
                self.content = file.read()
        elif content:
            self.filepath = None
            self.content = content
        else:
            raise NotImplementedError

    def execute(
        self,
        cursor: Cursor[Any],
        params: CursorParams,
    ) -> Cursor[Any]:
        self.logger.debug(self.content)
        cursor.execute(self.content, params)
        return cursor

    def executemany(
        self,
        cursor: Cursor[Any],
        params: Sequence[CursorParams],
    ) -> Cursor[Any]:
        self.logger.debug(self.content)
        cursor.executemany(self.content, params)
        return cursor
