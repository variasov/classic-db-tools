from .types import Connection

class Query:

    def __init__(self, template, renderer):
        self.template = template
        self.rendered = None
        self.parameters = None
        self.renderer = renderer

    def _render(self, **kwargs: object):
        self.rendered, self.parameters = self.renderer.prepare_query(
            self.template, **kwargs,
        )

    def _execute(self, connect: Connection, **kwargs: object):
        self._render(**kwargs)

        with connect.cursor() as cursor:
            cursor.execute(self.rendered, self.parameters)

            return cursor

    def many(self, connect: Connection, **kwargs: object):
        cursor = self._execute(connect, **kwargs)
        return cursor.fetchall()

    def one(self, connect: Connection, **kwargs: object):
        cursor = self._execute(connect, **kwargs)
        if cursor.rowcount == 0:
            return None
        return cursor.fetchone()

    def scalar(self, connect: Connection, **kwargs: object):
        cursor = self._execute(connect, **kwargs)
        if cursor.rowcount == 0:
            return None
        return cursor.fetchone()[0]

    def first(self, connect: Connection, **kwargs: object):
        cursor = self._execute(connect, **kwargs)
        if cursor.rowcount == 0:
            # TODO: вызвать ошибку
            return None
        return cursor.fetchone()

    def cursor(self, connect: Connection, **kwargs: object):
        return self._execute(connect, **kwargs)
