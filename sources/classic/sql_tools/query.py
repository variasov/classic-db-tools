from typing import Optional

from jinja2 import Template

from .templates import Renderer
from .params_styles import ParamStyleRecognizer
from .result import Result


class Query:

    def __init__(
            self, template: Template,
            renderer: Renderer,
            param_style_recognizer: ParamStyleRecognizer,
    ):
        self.template = template
        self.sql = None
        self.parameters = None
        self.renderer = renderer
        self._recognize_param_style = param_style_recognizer.get

    def execute(
        self,
        conn_or_cursor,
        batch_params: Optional[list[object]] = None,
        /,
        **kwargs: object,
    ) -> Result:
        if hasattr(conn_or_cursor, 'cursor'):
            cursor = conn_or_cursor.cursor()
            param_style = self._recognize_param_style(conn_or_cursor)
        else:
            cursor = conn_or_cursor
            param_style = self._recognize_param_style(conn_or_cursor.connection)

        sql, ordered_params = self.renderer.prepare_query(
            self.template, param_style, kwargs or {},
        )

        if batch_params is not None:
            assert not kwargs, ('Only batch_params or kwargs '
                                'allowed at the same time')
            cursor.executemany(sql, batch_params)
        else:
            cursor.execute(sql, ordered_params)

        return Result(cursor)

    __call__ = execute
