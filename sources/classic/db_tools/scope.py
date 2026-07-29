import threading
from typing import Any, Dict

from .types import Connection


class Scope(threading.local):
    """
    Tread-local объект, содержащий соединение с БД
    и параметры транзакции во время работы методов
    Engine.conn и Engine.transaction.

    Инстанцируется движком во время работы.
    """

    conn: Connection | None
    tx_params: Dict[str, Any] | None

    def __init__(self):
        super().__init__()
        self.conn = None
        self.tx_params = None
