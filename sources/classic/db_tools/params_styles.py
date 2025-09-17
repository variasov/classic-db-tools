import sys


known_styles = {}


def recognize_param_style(conn: object) -> str:
    conn_cls = conn.__class__

    try:
        return known_styles[conn_cls]
    except KeyError:
        modname = conn_cls.__module__
        while modname:
            try:
                style = sys.modules[modname].paramstyle  # type: ignore
                known_styles[conn_cls] = style
                return style
            except AttributeError:
                if "." in modname:
                    modname = modname.rsplit(".", 1)[0]
                else:
                    break
    raise TypeError(f"Can't find paramstyle for connection {conn!r}")
