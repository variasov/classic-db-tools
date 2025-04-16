import os
from abc import ABC, abstractmethod
from typing import Sequence
from jinja2 import Template


def make_template_path(templates_path: str) -> str:
    current_dir = os.path.dirname(__file__)
    return os.path.join(
        current_dir,
        'mapper_templates',
        templates_path,
    )


class Mapper(ABC):
    name: str
    identity_map_name: str

    @abstractmethod
    def to_sources(self, offsets: int):
        pass


class ToDict(Mapper):

    def __init__(
        self,
        name: str,
        id: str | Sequence[str],
        cols: dict[str, str],
    ):
        self.name = name
        self.identity_map_name = self.name + 's'
        self.id = id
        self.cols = cols

    def to_sources(self, offsets: int):
        template_path = make_template_path('to_dict_template.j2')
        with open(template_path) as file:
            template = Template(file.read())
        return template.render(
            name=self.name,
            identity_map_name=self.identity_map_name,
            id=self.id,
            cols=self.cols,
            offsets=offsets,
        ).splitlines()


class OneToMany:

    def __init__(
        self,
        left: str,
        attr: str,
        right: str,
    ):
        self.left = left
        self.attr = attr
        self.right = right

    def to_sources(self, offsets: int):
        template_path = make_template_path('one_to_many_template.j2')
        with open(template_path) as file:
            template = Template(file.read())
        return template.render(
            left=self.left,
            attr=self.attr,
            right=self.right,
            offsets=offsets
        ).splitlines()


def returning(*mappers, returns: str):
    template_path = make_template_path('returning_template.j2')
    with open(template_path) as file:
        template = Template(file.read())

    def obj_is_mapper(obj):
        return isinstance(obj, Mapper)

    func_source = template.render(
        mappers=mappers,
        returns=returns,
        obj_is_mapper=obj_is_mapper
    )

    code = compile(func_source, '<string>', 'exec')
    namespace = {}
    exec(code, namespace)
    return namespace['mapper_func']
