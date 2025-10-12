import os
import threading

from .query import StaticQuery


class StaticQueriesFactory:

    def __init__(self, templates_path: str):
        self.cache = {}
        self.templates_path = templates_path
        self.lock = threading.Lock()

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

            with self.lock:
                self.cache[key] = obj

        return obj
