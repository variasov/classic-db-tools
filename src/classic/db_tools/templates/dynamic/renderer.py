import re
import threading
from collections.abc import Callable, Iterable
from typing import Any, cast

from jinja2 import Template
from markupsafe import Markup


class Renderer(threading.local):
    """
    Производит рендер параметров в динамическом запросе.

    Используется под капотом DynamicTemplate, снаружи недоступен.
    """

    bind_params: dict[str, object]
    bind_values: list[object]
    param_style: str
    param_index: int

    def _auto_name(self, base: str | None) -> str:
        """Уникальное имя параметра: очищенный base + порядковый номер."""
        base = re.sub(r"[^A-Za-z0-9_]", "_", base or "") or "param"
        name = f"{base}_{self.param_index}"
        while name in self.bind_params:
            name += "_"
        return name

    def _bind_param(
        self,
        value: Any,  # значение параметра запроса
        name: str | None = None,
        auto: bool = True,
    ) -> str:
        """
        Регистрирует значение и возвращает плейсхолдер.

        auto=True: name - подсказка, к ней добавляется порядковый номер,
        имя уникально в рамках рендера. auto=False: name - явное имя,
        используется как есть.
        """
        self.param_index += 1
        if auto or not name:
            new_key = self._auto_name(name if auto else None)
        else:
            new_key = name

        param_style = self.param_style
        if param_style in ("named", "pyformat"):
            if new_key in self.bind_params:
                old = self.bind_params[new_key]
                if old is not value and old != value:
                    raise ValueError(
                        f'Bind parameter name "{new_key}" is used twice with '
                        f"different values ({old!r} and {value!r})"
                    )
            self.bind_params[new_key] = value
        else:
            self.bind_values.append(value)

        if param_style == "qmark":
            return "?"
        if param_style == "format":
            return "%s"
        if param_style == "numeric":
            return f":{self.param_index}"
        if param_style == "named":
            return f":{new_key}"
        if param_style == "pyformat":
            return f"%({new_key})s"
        if param_style == "asyncpg":
            return f"${self.param_index}"
        raise AssertionError(f"Invalid param_style - {param_style}")

    def sql_safe(self, value: Any) -> Markup:  # любое значение
        """Filter to mark the value of an expression as safe for inserting
        in a SQL statement"""
        return Markup(value)

    def bind(
        self,
        value: Any,  # значение параметра запроса
        name: str | None = None,
    ) -> str | Markup:
        """A filter that prints %s, and stores the value
        in an array, so that it can be bound using a prepared statement

        Явное имя используется как есть; без имени оно генерируется.
        """
        if isinstance(value, Markup):
            return value
        return self._bind_param(value, name, auto=name is None)

    def autobind(
        self,
        value: Any,  # значение параметра запроса
        name: str = "",
    ) -> str | Markup:
        """Filter automatically applied to every {{variable}}
        during the lexing stage, so developers can't forget to bind.

        name - текст выражения, к нему добавляется уникальный суффикс.
        """
        if isinstance(value, Markup):
            return value
        return self._bind_param(value, name, auto=True)

    def bind_in_clause(self, value: Iterable[Any]) -> str:
        values = list(value)
        if not values:
            return "(NULL)"
        results = [self._bind_param(v, "inclause") for v in values]

        clause = ",".join(results)
        return "(" + clause + ")"

    def build_escape_identifier_filter(
        self,
        quote_char: str,
    ) -> Callable[[Any], Markup]:
        def quote_and_escape(value: str) -> str:
            # Escape double quote with 2 double quotes,
            # or escape backtick with 2 backticks
            return (
                f"{quote_char}"
                f"{value.replace(quote_char, quote_char * 2)}"
                f"{quote_char}"
            )

        def identifier_filter(raw_identifier: Any) -> Markup:  # str/Iterable
            if isinstance(raw_identifier, str):
                raw_identifier = (raw_identifier,)
            if not isinstance(raw_identifier, Iterable):
                raise ValueError(
                    "identifier filter expects a string or an Iterable"
                )
            parts = cast("Iterable[str]", raw_identifier)
            return Markup(".".join(quote_and_escape(s) for s in parts))

        return identifier_filter

    def prepare_query(
        self,
        template: Template,
        data: dict[str, object],
        param_style: str,
    ) -> tuple[str, dict[str, object] | list[object]]:
        self.bind_params = {}
        self.bind_values = []
        self.param_style = param_style
        self.param_index = 0
        try:
            query = template.render(data)
            if self.param_style in ("named", "pyformat"):
                bind_params = dict(self.bind_params)
            elif self.param_style in ("qmark", "numeric", "format", "asyncpg"):
                bind_params = list(self.bind_values)
            else:
                raise NotImplementedError
            return query, bind_params
        finally:
            del self.bind_params
            del self.bind_values
            del self.param_style
            del self.param_index
