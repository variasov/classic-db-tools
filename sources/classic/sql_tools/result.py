from .types import Cursor


class Result:
    cursor: Cursor

    def __init__(self, cursor: Cursor):
        self.cursor = cursor

    def many(self):
        return self.cursor.fetchall()

    def one(self):
        return self.cursor.fetchone()

    def one_or_none(self):
        if self.cursor.rowcount == 0:
            return None
        return self.cursor.fetchone()

    def scalar(self):
        return self.cursor.fetchone()[0]
