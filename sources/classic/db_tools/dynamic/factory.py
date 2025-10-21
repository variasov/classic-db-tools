import threading
from typing import Iterable, Callable, Sequence

import jinja2

from classic.db_tools.params_styles import recognize_param_style
from classic.db_tools.types import Cursor, CursorParams

from .renderer import Renderer
from .extension import AutoBind


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
            self.template, params, recognize_param_style(cursor),
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
                self.template, param, recognize_param_style(cursor),
            )
            cursor.execute(sql, ordered_params)
        return cursor


class DynamicQueriesCache:
    VALID_ID_QUOTE_CHARS = ('`', "'")

    def __init__(
        self,
        templates_paths: Sequence[str],
        identifier_quote_char: str = "'",
    ):
        assert identifier_quote_char in self.VALID_ID_QUOTE_CHARS

        self.identifier_quote_char = identifier_quote_char
        self.jinja = jinja2.Environment(
            loader=jinja2.FileSystemLoader(templates_paths),
            auto_reload=False,
            autoescape=True,
        )
        self.renderer = Renderer()
        self.jinja.add_extension(AutoBind)
        self.jinja.filters['bind'] = self.renderer.bind
        self.jinja.filters['sqlsafe'] = self.renderer.sql_safe
        self.jinja.filters['inclause'] = self.renderer.bind_in_clause
        self.jinja.filters['identifier'] = (
            self.renderer.build_escape_identifier_filter(
                self.identifier_quote_char,
            )
        )
        self.cache = {}
        self.lock = threading.RLock()

    def create_lazy(
        self,
        filename: str = None,
        content: str = None,
    ) -> Callable[[], DynamicQuery]:
        if filename:
            key = filename
        elif content:
            key = content
        else:
            raise NotImplemented

        def lazy_query():
            with self.lock:
                obj = self.cache.get(key)
                if obj is None:
                    if filename:
                        template = self.jinja.get_template(filename)
                    elif content:
                        template = self.jinja.from_string(content)
                    else:
                        raise NotImplemented

                    obj = DynamicQuery(self.renderer, template)
                    self.cache[key] = obj

            return obj

        return lazy_query
