from pathlib import Path
from typing import Sequence, Callable

import os
import threading

from classic.db_tools.types import Cursor, CursorParams


class StaticQuery:

    def __init__(
        self,
        filepath: str = None,
        content: str = None,
    ):
        assert filepath is None or content is None
        if filepath:
            self.filepath = filepath
            with open(self.filepath, 'rt') as file:
                self.content = file.read()
        elif content:
            self.filepath = None
            self.content = content
        else:
            raise NotImplemented

    def execute(
        self,
        params: CursorParams = None,
        cursor: Cursor = None,
    ) -> Cursor:
        cursor.execute(self.content, params)
        return cursor

    def executemany(
        self,
        params: Sequence[CursorParams],
        cursor: Cursor = None,
    ) -> Cursor:
        cursor.executemany(self.content, params)
        return cursor


class StaticQueriesCache:

    def __init__(self, templates_paths: Sequence[str]):
        self.cache = {}
        self.templates_paths = templates_paths
        self.lock = threading.RLock()

    def create_lazy(
        self,
        filename: str = None,
        content: str = None,
    ) ->  Callable[[], StaticQuery]:
        if filename:
            key = filename
        elif content:
            key = content
        else:
            raise NotImplemented

        def lazy_query():
            with self.lock:
                obj = self.cache.get(key)
                if obj is None:
                    if filename:
                        for path in self.templates_paths:
                            filepath = os.path.join(path, filename)
                            if os.path.exists(filepath):
                                obj = StaticQuery(filepath=filepath)
                                break
                        if obj is None:
                            raise FileNotFoundError(
                                f'File {filename} does not exist in {self.templates_paths} dirs'
                            )
                    elif content:
                        obj = StaticQuery(content=content)
                    else:
                        raise NotImplemented

                    with self.lock:
                        self.cache[key] = obj

            return obj

        return lazy_query
