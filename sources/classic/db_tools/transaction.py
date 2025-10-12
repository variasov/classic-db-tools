from types import TracebackType

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
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if type_ is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        if self.return_autocommit_initial:
            self.conn.autocommit = True

        return False
