from abc import ABC, abstractmethod
from ast import *
from typing import Sequence


COL_OFFSET = ' ' * 4


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
        return [
            COL_OFFSET * offsets + f"{self.name} = {self.identity_map_name}.get(row['{self.id}'])",
            COL_OFFSET * offsets + f"if {self.name} is None:",
            COL_OFFSET * (offsets + 1) + f"{self.name} = {self.identity_map_name}[row['{self.id}']] = {{",
            *(
                COL_OFFSET * (offsets + 2) + f"'{key}': row['{col}'],"
                for key, col in self.cols.items()
            ),
            COL_OFFSET * (offsets + 1) + '}',
        ]


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
        return [
            COL_OFFSET * offsets + f"if '{self.attr}' not in {self.left}:",
            COL_OFFSET * (offsets + 1) + f"{self.left}['{self.attr}'] = []",
            COL_OFFSET * offsets + f"{self.left}['{self.attr}'].append({self.right})",
        ]


def returning(*mappers, returns: str):
    func_body = [f'def mapper_func(rows):']
    for obj in mappers:
        if isinstance(obj, Mapper):
            func_body.append(
                f'{COL_OFFSET}{obj.identity_map_name} = {{}}'
            )

    func_body.append(f'{COL_OFFSET}for row in rows:')

    for obj in mappers:
        func_body.extend(obj.to_sources(offsets=2))

    func_body.append(f'{COL_OFFSET}return list({returns}s.values())')

    func_source = '\n'.join(func_body)

    code = compile(func_source, '<string>', 'exec')
    namespace = {}
    exec(code, namespace)
    return namespace['mapper_func']
