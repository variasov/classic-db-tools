import logging
from typing import Optional, Sequence, Union

import os
import threading

from .template import StaticTemplate


class StaticTemplatesCache:

    def __init__(
        self,
        logger: logging.Logger,
        templates_paths: Sequence[Union[str, os.PathLike]],
    ):
        self.logger = logger
        self.cache = {}
        self.templates_paths = templates_paths
        self.lock = threading.RLock()

    def get_or_create(
        self,
        filename: Optional[str] = None,
        content: Optional[str] = None,
    ) -> StaticTemplate:
        if filename:
            key = filename
        elif content:
            key = content
        else:
            raise NotImplemented

        with self.lock:
            obj = self.cache.get(key)
            if obj is None:
                if filename:
                    for path in self.templates_paths:
                        filepath = os.path.join(path, filename)
                        if os.path.exists(filepath):
                            obj = StaticTemplate(self.logger, filepath=filepath)
                            break
                    if obj is None:
                        raise FileNotFoundError(
                            f'File {filename} does not exist in '
                            f'{self.templates_paths} dirs'
                        )
                elif content:
                    obj = StaticTemplate(self.logger, content=content)
                else:
                    raise NotImplemented

                self.cache[key] = obj

        return obj
