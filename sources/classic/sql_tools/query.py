from typing import Optional, Iterable

from jinja2 import Template

from .types import Connection
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
        conn: Connection,
        params: Optional[list[object] | dict[str, object]] = None,
    ) -> Result:
        param_style = self._recognize_param_style(conn)
        cursor = conn.cursor()

        if isinstance(params, dict) or params is None:
            sql, ordered_params = self.renderer.prepare_query(
                self.template, param_style, params or {},
            )
            cursor.execute(sql, ordered_params)
        elif isinstance(params, Iterable):
            sql, _ = self.renderer.prepare_query(
                self.template, param_style, {},
            )
            cursor.executemany(sql, params)
        else:
            raise ValueError('Params must be dict or iterable of dicts')

        return Result(cursor)

    def many(self, conn: Connection, **kwargs: object) -> list[object]:
        return self.execute(conn, kwargs).many()

    def one(self, conn: Connection, **kwargs: object) -> object:
        return self.execute(conn, kwargs).one()

    def one_or_none(self, conn: Connection, **kwargs: object) -> object | None:
        return self.execute(conn, kwargs).one_or_none()

    def scalar(self, conn: Connection, **kwargs: object) -> object:
        return self.execute(conn, kwargs).scalar()
