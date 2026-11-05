from typing import ClassVar

from .dbapi import DBModule


class ConnectionValidator:
    validators: ClassVar[dict[DBModule, type["ConnectionValidator"]]] = {}

    def __init_subclass__(cls, driver: DBModule, **kwargs):
        cls.validators[driver] = cls

    def validate(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except Exception:
            return False
        return True

    def before_release(self, conn):
        try:
            conn.rollback()
        except Exception:
            return False
        return self.validate(conn)
