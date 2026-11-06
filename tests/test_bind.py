import logging
import re
from typing import Any

import pytest

from classic.db_tools import Engine
from classic.db_tools.templates.dynamic.cache import DynamicTemplatesCache

pytestmark = pytest.mark.usefixtures("data")

STYLES = ("qmark", "format", "numeric", "asyncpg", "named", "pyformat")
POSITIONAL = ("qmark", "format", "numeric", "asyncpg")
NAMED = ("named", "pyformat")

MACRO = (
    "{% macro cond(c) %}value = {{ c.value }}{% endmacro %}"
    "SELECT title FROM items WHERE {{ cond(one) }} OR {{ cond(two) }}"
)


def render(
    content: str,
    style: str,
    **data: Any,
) -> tuple[str, Any]:  # params: dict (named styles) or list (positional)
    cache = DynamicTemplatesCache(
        logging.getLogger("test"),
        [],
        style,
    )
    tmpl = cache.get_or_create(content=content)
    return cache.renderer.prepare_query(tmpl.template, data, style)


def names(sql: str, style: str) -> list[str]:
    if style == "named":
        return re.findall(r":(\w+)", sql)
    return re.findall(r"%\((\w+)\)s", sql)


@pytest.fixture
def items(engine: Engine[Any], data: None) -> None:
    for title, value in (("a", 1), ("b", 2), ("c", 3)):
        engine.query(
            "INSERT INTO items (title, value) VALUES ({{ t }}, {{ v }})",
            static=False,
        ).execute(t=title, v=value)


class Obj:
    def __init__(self, value: Any) -> None:
        self.value = value


class TestUniqueNames:
    @pytest.mark.parametrize("style", NAMED)
    def test_macro_twice_named(self, style: str):
        sql, params = render(MACRO, style, one=Obj(1), two=Obj(2))
        found = names(sql, style)
        assert len(found) == 2
        assert found[0] != found[1]
        assert [params[n] for n in found] == [1, 2]
        assert len(params) == 2

    @pytest.mark.parametrize("style", POSITIONAL)
    def test_macro_twice_positional(self, style: str):
        _sql, params = render(MACRO, style, one=Obj(1), two=Obj(2))
        assert params == [1, 2]

    def test_macro_twice_sqlite(self, engine: Engine[Any], items: None):
        rows = engine.query(MACRO, static=False).all(
            one=Obj(1),
            two=Obj(3),
        )
        assert sorted(r[0] for r in rows) == ["a", "c"]

    @pytest.mark.parametrize("style", POSITIONAL)
    def test_repeat_positional(self, style: str):
        sql, params = render("{{ a }}, {{ a }}, {{ a }}", style, a=5)
        assert params == [5, 5, 5]
        if style == "numeric":
            assert sql == ":1, :2, :3"
        elif style == "asyncpg":
            assert sql == "$1, $2, $3"

    @pytest.mark.parametrize("style", NAMED)
    def test_repeat_named(self, style: str):
        sql, params = render("{{ a }}, {{ a }}", style, a=5)
        found = names(sql, style)
        assert len(set(found)) == 2
        assert [params[n] for n in found] == [5, 5]

    @pytest.mark.parametrize("style", POSITIONAL)
    def test_loop_positional(self, style: str):
        _sql, params = render(
            "{% for v in vs %}{{ v }},{% endfor %}",
            style,
            vs=[7, 8, 9],
        )
        assert params == [7, 8, 9]

    @pytest.mark.parametrize("style", NAMED)
    def test_loop_named(self, style: str):
        sql, params = render(
            "{% for v in vs %}{{ v }},{% endfor %}",
            style,
            vs=[7, 8, 9],
        )
        found = names(sql, style)
        assert len(set(found)) == 3
        assert [params[n] for n in found] == [7, 8, 9]

    def test_loop_sqlite(self, engine: Engine[Any], items: None):
        rows = engine.query(
            "SELECT title FROM items WHERE value IN ("
            "{% for v in vs %}{{ v }}{% if not loop.last %},{% endif %}"
            "{% endfor %}) ORDER BY id",
            static=False,
        ).all(vs=[1, 3])
        assert [r[0] for r in rows] == ["a", "c"]

    @pytest.mark.parametrize("style", STYLES)
    def test_nameless_expressions(self, style: str):
        sql, params = render(
            "{{ '%' ~ t ~ '%' }} {{ '%' ~ t ~ '%' }} {{ 1 }}",
            style,
            t="x",
        )
        if style in NAMED:
            found = names(sql, style)
            assert len(set(found)) == 3
            assert all(re.fullmatch(r"\w+", n) for n in found)
            assert [params[n] for n in found] == ["%x%", "%x%", 1]
        else:
            assert params == ["%x%", "%x%", 1]


class TestInclause:
    @pytest.mark.parametrize("style", POSITIONAL)
    def test_two_inclauses_positional(self, style: str):
        _sql, params = render(
            "{{ a|inclause }} {{ b|inclause }}",
            style,
            a=[1, 2],
            b=[3],
        )
        assert params == [1, 2, 3]

    @pytest.mark.parametrize("style", NAMED)
    def test_two_inclauses_named(self, style: str):
        sql, params = render(
            "{{ a|inclause }} {{ b|inclause }}",
            style,
            a=[1, 2],
            b=[3],
        )
        found = names(sql, style)
        assert len(set(found)) == 3
        assert [params[n] for n in found] == [1, 2, 3]

    def test_two_inclauses_sqlite(self, engine: Engine[Any], items: None):
        rows = engine.query(
            "SELECT title FROM items WHERE id IN {{ ids|inclause }} "
            "OR value IN {{ vals|inclause }} ORDER BY id",
            static=False,
        ).all(ids=[1], vals=[3])
        assert [r[0] for r in rows] == ["a", "c"]

    @pytest.mark.parametrize("style", STYLES)
    def test_empty_inclause(self, style: str):
        sql, params = render("x IN {{ a|inclause }}", style, a=[])
        assert sql == "x IN (NULL)"
        assert not params

    def test_empty_inclause_sqlite(self, engine: Engine[Any], items: None):
        rows = engine.query(
            "SELECT title FROM items WHERE id IN {{ ids|inclause }}",
            static=False,
        ).all(ids=[])
        assert rows == []


class TestExplicitBind:
    @pytest.mark.parametrize("style", STYLES)
    def test_explicit_name_bound_once(self, style: str):
        sql, params = render("{{ x | bind('p1') }}", style, x=5)
        if style in NAMED:
            assert params == {"p1": 5}
            assert names(sql, style) == ["p1"]
        else:
            assert params == [5]

    @pytest.mark.parametrize("style", STYLES)
    def test_explicit_name_with_sqlsafe(self, style: str):
        _sql, params = render(
            "{{ x | bind('p1') | sqlsafe }}",
            style,
            x=5,
        )
        if style in NAMED:
            assert params == {"p1": 5}
        else:
            assert params == [5]

    @pytest.mark.parametrize("style", STYLES)
    def test_bind_name_expression(self, style: str):
        _sql, params = render(
            "{{ x | bind('p_' ~ i) }}",
            style,
            x=5,
            i=3,
        )
        assert params == ({"p_3": 5} if style in NAMED else [5])

    @pytest.mark.parametrize("style", STYLES)
    def test_bare_bind(self, style: str):
        sql, params = render("{{ x | bind }}", style, x=5)
        if style in NAMED:
            assert list(params.values()) == [5]
            assert len(names(sql, style)) == 1
        else:
            assert params == [5]

    @pytest.mark.parametrize("style", NAMED)
    def test_same_name_different_values(self, style: str):
        with pytest.raises(ValueError, match="p1"):
            render(
                "{{ x | bind('p1') }} {{ y | bind('p1') }}",
                style,
                x=1,
                y=2,
            )

    @pytest.mark.parametrize("style", NAMED)
    def test_same_name_same_values(self, style: str):
        _sql, params = render(
            "{{ x | bind('p1') }} {{ y | bind('p1') }}",
            style,
            x=1,
            y=1,
        )
        assert params == {"p1": 1}

    @pytest.mark.parametrize("style", POSITIONAL)
    def test_same_name_positional(self, style: str):
        _sql, params = render(
            "{{ x | bind('p1') }} {{ y | bind('p1') }}",
            style,
            x=1,
            y=2,
        )
        assert params == [1, 2]

    def test_explicit_bind_sqlite(self, engine: Engine[Any], items: None):
        rows = engine.query(
            "SELECT title FROM items WHERE value = {{ v | bind('p1') }}",
            static=False,
        ).all(v=2)
        assert [r[0] for r in rows] == ["b"]

    @pytest.mark.parametrize("style", STYLES)
    def test_sqlsafe_not_bound(self, style: str):
        sql, params = render("{{ x | sqlsafe }}", style, x="1 + 2")
        assert sql == "1 + 2"
        assert not params
