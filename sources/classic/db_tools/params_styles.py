import sys

from classic.db_tools.types import Connection, Cursor

known_styles = {}


def recognize_param_style(obj: Connection | Cursor) -> str:
    cls = obj.__class__

    try:
        return known_styles[cls]
    except KeyError:
        modname = cls.__module__
        while modname:
            try:
                style = sys.modules[modname].paramstyle  # type: ignore
                known_styles[cls] = style
                return style
            except AttributeError:
                if "." in modname:
                    modname = modname.rsplit(".", 1)[0]
                else:
                    break
    raise TypeError(f"Can't find paramstyle for connection {obj!r}")
