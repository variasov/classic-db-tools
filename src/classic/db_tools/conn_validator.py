from typing import Any, ClassVar

from .dbapi import DBModule


class ConnectionValidator:
    validators: ClassVar[dict[DBModule, type["ConnectionValidator"]]] = {}

    def __init_subclass__(cls, driver: DBModule, **kwargs: Any) -> None:
        cls.validators[driver] = cls

    def validate(self, conn: Any) -> bool:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except Exception:  # любая ошибка = соединение негодно
            return False
        return True

    def before_release(self, conn: Any) -> bool:
        try:
            conn.rollback()
        except Exception:  # любая ошибка = соединение негодно
            return False
        return self.validate(conn)
