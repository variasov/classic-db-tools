import logging
from typing import Any, Dict, Sequence

import jinja2

from classic.db_tools.types import Cursor

from .renderer import Renderer

from ..template import Template


class DynamicTemplate(Template):
    """
    Динамический SQL-шаблон.

    Используется под капотом Query и MapperQuery, снаружи недоступен.
    """


    def __init__(
        self,
        logger: logging.Logger,
        renderer: Renderer,
        template: jinja2.Template,
        param_style: str,
    ):
        self.logger = logger
        self.renderer = renderer
        self.template = template
        self.param_style = param_style

    def execute(
        self,
        cursor: Cursor,
        params: Dict[str, Any],
    ) -> Cursor:
        sql, ordered_params = self.renderer.prepare_query(
            self.template, params, self.param_style,
        )
        self.logger.debug(sql)
        cursor.execute(sql, ordered_params)
        return cursor

    def executemany(
        self,
        cursor: Cursor,
        params: Sequence[Dict[str, Any]],
    ) -> Cursor:
        for param in params:
            sql, ordered_params = self.renderer.prepare_query(
                self.template, param, self.param_style,
            )
            self.logger.debug(sql)
            cursor.execute(sql, ordered_params)
        return cursor
