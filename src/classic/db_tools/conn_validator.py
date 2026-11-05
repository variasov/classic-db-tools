from typing import ClassVar, Dict, Type

from .dbapi import DBModule


class ConnectionValidator:
    validators: ClassVar[Dict[DBModule, Type['ConnectionValidator']]] = {}

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
