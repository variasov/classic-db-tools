import logging
from os import PathLike
import threading
from typing import Sequence, Optional, Union

import jinja2

from .renderer import Renderer
from .extension import AutoBind

try:
    from .criteria_macro import register_criteria_macro
except ImportError:
    register_criteria_macro = None

from .template import DynamicTemplate


class DynamicTemplatesCache:
    """
    Кеш со динамическими шаблонами.

    Используется под капотом Engine, снаружи не доступен.
    """

    VALID_ID_QUOTE_CHARS = ('"', '`', "'")

    def __init__(
        self,
        logger: logging.Logger,
        templates_paths: Sequence[Union[str, PathLike]],
        paramstyle: str,
        identifier_quote_char: Optional[str] = None,
    ):
        self.identifier_quote_char = identifier_quote_char or '"'
        self.paramstyle = paramstyle

        assert self.identifier_quote_char in self.VALID_ID_QUOTE_CHARS

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
        if register_criteria_macro is not None:
            register_criteria_macro(self.jinja)

        self.cache = {}
        self.lock = threading.RLock()
        self.logger = logger

    def get_or_create(
        self,
        filename: Optional[str] = None,
        content: Optional[str] = None,
    ) -> DynamicTemplate:
        if filename:
            key = filename
        elif content:
            key = content
        else:
            raise NotImplementedError

        with self.lock:
            obj = self.cache.get(key)
            if obj is None:
                if filename:
                    template = self.jinja.get_template(filename)
                elif content:
                    template = self.jinja.from_string(content)
                else:
                    raise NotImplementedError

                obj = DynamicTemplate(
                    self.logger, self.renderer,
                    template, self.paramstyle,
                )
                self.cache[key] = obj

        return obj
