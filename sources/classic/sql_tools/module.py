from jinja2 import Environment, FileSystemLoader

from .templates import Renderer, AutoBind
from .params_styles import ParamStyleRecognizer
from .query import Query


class Module:
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
            auto_reload: bool = False,
            identifier_quote_character: str = "'",
    ):
        assert identifier_quote_character in self.VALID_ID_QUOTE_CHARS
        self.identifier_quote_character = identifier_quote_character

        self.renderer = Renderer()

        self.templates = Environment(
            loader=FileSystemLoader(templates_path),
            auto_reload=auto_reload,
            autoescape=True,
        )
        self.templates.add_extension(AutoBind)
        self.templates.filters['bind'] = self.renderer.bind
        self.templates.filters['sqlsafe'] = self.renderer.sql_safe
        self.templates.filters['inclause'] = self.renderer.bind_in_clause
        self.templates.filters['identifier'] = (
            self.renderer.build_escape_identifier_filter(
                self.identifier_quote_character,
            )
        )
        self.param_style_recognizer = ParamStyleRecognizer()

    def from_file(self, filename: str) -> Query:
        template = self.templates.get_template(filename)
        return Query(template, self.renderer, self.param_style_recognizer)

    def from_str(self, query: str) -> Query:
        template = self.templates.from_string(query)
        return Query(template, self.renderer, self.param_style_recognizer)
