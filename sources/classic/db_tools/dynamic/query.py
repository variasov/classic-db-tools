from typing import Iterable

import jinja2

from classic.db_tools.types import Cursor, CursorParams

from .renderer import Renderer


class DynamicQuery:

    def __init__(
        self,
        renderer: Renderer,
        template: jinja2.Template,
    ):
        self.renderer = renderer
        self.template = template

    def execute(
        self,
        params: CursorParams = None,
        cursor: Cursor = None,
    ) -> Cursor:
        sql, ordered_params = self.renderer.prepare_query(
            self.template, params,
        )
        cursor.execute(sql, ordered_params)
        return cursor

    def executemany(
        self,
        params: Iterable[CursorParams],
        cursor: Cursor,
    ) -> Cursor:
        for param in params:
            sql, ordered_params = self.renderer.prepare_query(
                self.template, param,
            )
            cursor.execute(sql, ordered_params)
        return cursor
