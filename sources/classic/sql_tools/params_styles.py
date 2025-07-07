import sys

try:
    from classic.db_utils import ScopedConnection
except ImportError:
    ScopedConnection = None


class ParamStyleRecognizer:
    known_styles: dict[type, str]

    def __init__(self):
        self.known_styles = {}

    def get(self, conn: object) -> str:
        if ScopedConnection is not None and isinstance(conn, ScopedConnection):
            conn_cls = conn.wrapped_conn_cls
        else:
            conn_cls = conn.__class__

        try:
            return self.known_styles[conn_cls]
        except KeyError:
            modname = conn_cls.__module__
            while modname:
                try:
                    style = sys.modules[modname].paramstyle  # type: ignore
                    self.known_styles[conn_cls] = style
                    return style
                except AttributeError:
                    if "." in modname:
                        modname = modname.rsplit(".", 1)[0]
                    else:
                        break
        raise TypeError(f"Can't find paramstyle for connection {conn!r}")
