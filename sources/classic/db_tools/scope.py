import threading

from .types import Connection


class Scope(threading.local):
    conn: Connection | None
    depth: int
    tx_depth: int
    tx: 'Transaction' | None

    def __init__(self):
        super().__init__()
        self.conn = None
        self.depth = 0
        self.tx_depth = 0
        self.tx = None
