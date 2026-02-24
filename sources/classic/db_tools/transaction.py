from types import TracebackType
from typing import Optional, Type

from .types import Connection


class Transaction:

    def __init__(self, conn: Connection):
        self.conn = conn

    def __enter__(self):
        self.return_autocommit_initial = self.conn.autocommit
        if self.conn.autocommit is True:
            self.conn.autocommit = False
        return self

    def __exit__(
            self,
            type_: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if type_ is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        if self.return_autocommit_initial:
            self.conn.autocommit = True

        return False
