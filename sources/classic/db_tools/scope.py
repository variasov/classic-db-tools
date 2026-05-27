import threading
from typing import Any, Dict

from .types import Connection


class Scope(threading.local):
    conn: Connection | None
    tx_params: Dict[str, Any] | None

    def __init__(self):
        super().__init__()
        self.conn = None
        self.tx_params = None
