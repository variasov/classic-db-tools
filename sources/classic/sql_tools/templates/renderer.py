import threading
from collections.abc import Iterable

from markupsafe import Markup
from jinja2 import Template


class Renderer(threading.local):
    bind_params: dict[str, object]
    param_style: str
    param_index: int

    def _bind_param(self, already_bound, key, value):
        self.param_index += 1
        # TODO: придумать, как с этим жить
        #new_key = '%s_%s' % (key, self.param_index)
        new_key = key
        already_bound[new_key] = value

        param_style = self.param_style
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

    def bind(self, value, name):
        """A filter that prints %s, and stores the value
        in an array, so that it can be bound using a prepared statement

        This filter is automatically applied to every {{variable}}
        during the lexing stage, so developers can't forget to bind
        """
        if isinstance(value, Markup):
            return value
        else:
            return self._bind_param(self.bind_params, name, value)

    def bind_in_clause(self, value):
        values = list(value)
        results = []
        for v in values:
            results.append(
                self._bind_param(self._thread_local.bind_params, 'inclause', v)
            )

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
            return Markup('.'.join(quote_and_escape(s) for s in raw_identifier))

        return identifier_filter

    def prepare_query(
        self,
        template: Template,
        param_style: str,
        data: dict[str, object],
    ):
        self.bind_params = {}
        self.param_style = param_style
        self.param_index = 0
        try:
            query = template.render(data)
            if param_style in ('named', 'pyformat'):
                bind_params = dict(self.bind_params)
            elif param_style in ('qmark', 'numeric', 'format', 'asyncpg'):
                bind_params = list(self.bind_params.values())
            else:
                raise NotImplemented
            return query, bind_params
        finally:
            del self.bind_params
            del self.param_style
            del self.param_index
