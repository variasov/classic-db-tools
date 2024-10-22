from .query import Query
from jinja2 import Environment, PackageLoader

from .template_renderer import JinjaSql


class Module:

    def __init__(self, templates_path: str):
        self.templates_path = templates_path
        self.templates = Environment(
            # TODO: посмотреть варианты другого лоадера
            loader=PackageLoader('example.api', templates_path),
            autoescape=False,
            auto_reload=False,
        )
        self.renderer = JinjaSql(self.templates)


    def from_file(self, filename: str) -> Query:
        template = self.templates.get_template(filename)
        return Query(template, self.renderer)

    def from_str(self, query: str) -> Query:
        pass
