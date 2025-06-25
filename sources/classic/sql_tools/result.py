

class Result:

    def __init__(self, cursor):
        self.cursor = cursor

    def many(self, batch_size: int = None):
        if batch_size:
            return self.cursor.fetchmany(batch_size)
        return self.cursor.fetchall()

    def one(self, raising: bool = False):
        value = self.cursor.fetchone()
        if raising and value is None:
            raise ValueError
        else:
            return value

    def scalar(self, raising: bool = False):
        value = self.one(raising)
        if not raising and value is None:
            return None
        return value[0]
