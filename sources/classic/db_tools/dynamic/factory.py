from jinja2 import Environment, FileSystemLoader

from .renderer import Renderer
from .extension import AutoBind
from .query import DynamicQuery


class DynamicQueriesFactory:
    VALID_ID_QUOTE_CHARS = ('`', "'")
    VALID_PARAM_STYLES = (
        'qmark',     # qmark 'where name = ?'
        'numeric',   # numeric 'where name = :1'
        'named',     # named 'where name = :name'
        'format',    # format 'where name = %s'
        'pyformat',  # pyformat 'where name = %(name)s'
        'asyncpg',   # asyncpg 'where name = $1'
    )

    def __init__(
        self,
        templates_path: str,
        param_style: str,
        identifier_quote_char: str = "'",
    ):
        assert identifier_quote_char in self.VALID_ID_QUOTE_CHARS
        assert param_style in self.VALID_PARAM_STYLES

        self.identifier_quote_char = identifier_quote_char
        self.param_style = param_style
        self.jinja = Environment(
            loader=FileSystemLoader(templates_path),
            auto_reload=False,
            autoescape=True,
        )
        self.renderer = Renderer()
        self.renderer.param_style = self.param_style
        self.jinja.add_extension(AutoBind)
        self.jinja.filters['bind'] = self.renderer.bind
        self.jinja.filters['sqlsafe'] = self.renderer.sql_safe
        self.jinja.filters['inclause'] = self.renderer.bind_in_clause
        self.jinja.filters['identifier'] = (
            self.renderer.build_escape_identifier_filter(
                self.identifier_quote_char,
            )
        )

    def get(self, filename: str = None, content: str = None) -> DynamicQuery:
        assert filename is None or content is None
        if filename:
            template = self.jinja.get_template(filename)
            return DynamicQuery(self.renderer, template)
        elif content:
            template = self.jinja.from_string(content)
            return DynamicQuery(self.renderer, template)
        else:
            raise NotImplemented
