from collections.abc import Iterable
import re
import threading
from typing import Dict, List, Tuple, Union

from markupsafe import Markup
from jinja2 import Template


class Renderer(threading.local):
    """
    Производит рендер параметров в динамическом запросе.

    Используется под капотом DynamicTemplate, снаружи недоступен.
    """

    bind_params: Dict[str, object]
    bind_values: List[object]
    param_style: str
    param_index: int

    def _auto_name(self, base):
        """Уникальное имя параметра: очищенный base + порядковый номер."""
        base = re.sub(r'[^A-Za-z0-9_]', '_', base or '') or 'param'
        name = '%s_%s' % (base, self.param_index)
        while name in self.bind_params:
            name += '_'
        return name

    def _bind_param(self, value, name=None, auto=True):
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
        if param_style in ('named', 'pyformat'):
            if new_key in self.bind_params:
                old = self.bind_params[new_key]
                if old is not value and old != value:
                    raise ValueError(
                        'Bind parameter name "%s" is used twice with '
                        'different values (%r and %r)'
                        % (new_key, old, value)
                    )
            self.bind_params[new_key] = value
        else:
            self.bind_values.append(value)

        if param_style == 'qmark':
            return '?'
        elif param_style == 'format':
            return '%s'
        elif param_style == 'numeric':
            return ':%s' % self.param_index
        elif param_style == 'named':
            return ':%s' % new_key
        elif param_style == 'pyformat':
            return '%%(%s)s' % new_key
        elif param_style == 'asyncpg':
            return '$%s' % self.param_index
        else:
            raise AssertionError('Invalid param_style - %s' % param_style)

    def sql_safe(self, value):
        """Filter to mark the value of an expression as safe for inserting
        in a SQL statement"""
        return Markup(value)

    def bind(self, value, name=None):
        """A filter that prints %s, and stores the value
        in an array, so that it can be bound using a prepared statement

        Явное имя используется как есть; без имени оно генерируется.
        """
        if isinstance(value, Markup):
            return value
        return self._bind_param(value, name, auto=name is None)

    def autobind(self, value, name=''):
        """Filter automatically applied to every {{variable}}
        during the lexing stage, so developers can't forget to bind.

        name - текст выражения, к нему добавляется уникальный суффикс.
        """
        if isinstance(value, Markup):
            return value
        return self._bind_param(value, name, auto=True)

    def bind_in_clause(self, value):
        values = list(value)
        if not values:
            return '(NULL)'
        results = [self._bind_param(v, 'inclause') for v in values]

        clause = ','.join(results)
        clause = '(' + clause + ')'
        return clause

    def build_escape_identifier_filter(self, quote_char):
        def quote_and_escape(value):
            # Escape double quote with 2 double quotes,
            # or escape backtick with 2 backticks
            return (f'{quote_char}'
                    f'{value.replace(quote_char, quote_char * 2)}'
                    f'{quote_char}')

        def identifier_filter(raw_identifier):
            if isinstance(raw_identifier, str):
                raw_identifier = (raw_identifier, )
            if not isinstance(raw_identifier, Iterable):
                raise ValueError(
                    'identifier filter expects a string or an Iterable'
                )
            return Markup('.'.join(
                quote_and_escape(s) for s in raw_identifier
            ))

        return identifier_filter

    def prepare_query(
        self,
        template: Template,
        data: Dict[str, object],
        param_style: str,
    ) -> Tuple[str, Union[Dict[str, object], List[object]]]:
        self.bind_params = {}
        self.bind_values = []
        self.param_style = param_style
        self.param_index = 0
        try:
            query = template.render(data)
            if self.param_style in ('named', 'pyformat'):
                bind_params = dict(self.bind_params)
            elif self.param_style in ('qmark', 'numeric', 'format', 'asyncpg'):
                bind_params = list(self.bind_values)
            else:
                raise NotImplementedError
            return query, bind_params
        finally:
            del self.bind_params
            del self.bind_values
            del self.param_style
            del self.param_index
