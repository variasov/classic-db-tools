import os

from .query import StaticQuery


class StaticQueriesFactory:

    def __init__(self, templates_path: str):
        self.cache = {}
        self.templates_path = templates_path

    def get(self, filename: str = None, content: str = None) -> StaticQuery:
        if filename:
            key = filename
        elif content:
            key = content
        else:
            raise NotImplemented

        obj = self.cache.get(key)
        if obj is None:
            if filename:
                obj = StaticQuery(
                    filename=os.path.join(self.templates_path, filename),
                )
            elif content:
                obj = StaticQuery(content=content)
            else:
                raise NotImplemented

            self.cache[key] = obj

        return obj
