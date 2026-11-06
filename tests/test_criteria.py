from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from classic.criteria import And, Criteria, Or, ReturnsFalse, ReturnsTrue

from classic.db_tools import Engine
from classic.db_tools.templates.dynamic.criteria_macro import traverse


@dataclass
class TitleIs(Criteria[Any]):
    title: str


@dataclass
class ValueGt(Criteria[Any]):
    value: int


@dataclass
class IdGe(Criteria[Any]):
    min_id: int


@dataclass
class NoMacro(Criteria[Any]):
    pass


ITEMS = "criteria/items.sql.tmpl"


def _macro(text: str) -> Callable[[Criteria[Any]], str]:
    def macro(criteria: Criteria[Any]) -> str:
        return text

    return macro


@pytest.fixture
def items(engine: Engine[Any], data: None) -> None:
    for title, value in (("a", 10), ("b", 20), ("c", 30), ("d", 5)):
        engine.query_from("insert_item.sql").execute(t=title, v=value)


def titles(engine: Engine[Any], criteria: Criteria[Any]) -> list[str]:
    rows = engine.query_from(ITEMS).all(criteria=criteria)
    return [row[0] for row in rows]


class TestTraverse:
    def test_single(self, engine: Engine[Any], items: None):
        assert titles(engine, TitleIs("b")) == ["b"]

    def test_and_or_precedence(self, engine: Engine[Any], items: None):
        # Без скобок: title = a AND value > 15 OR id >= 3 -- неверно
        crit = TitleIs("a") & (ValueGt(15) | IdGe(3))
        assert titles(engine, crit) == []
        crit = TitleIs("a") & (ValueGt(5) | IdGe(100))
        assert titles(engine, crit) == ["a"]

    def test_or_of_and(self, engine: Engine[Any], items: None):
        crit = (TitleIs("c") | IdGe(100)) & ValueGt(15)
        assert titles(engine, crit) == ["c"]

    def test_invert_of_and(self, engine: Engine[Any], items: None):
        crit = ~(ValueGt(8) & IdGe(2))
        # a: not(T and F); b, c: not(T and T); d: not(F and T)
        assert titles(engine, crit) == ["a", "d"]

    def test_text(self):
        ns = SimpleNamespace(
            TitleIs=_macro("T"),
            ValueGt=_macro("V"),
        )
        crit = ~(TitleIs("a") & ValueGt(1))
        assert traverse(crit, ns) == "NOT ((T) AND (V))"
        crit = TitleIs("a") & (ValueGt(1) | ValueGt(2))
        assert traverse(crit, ns) == "(T) AND ((V) OR (V))"

    def test_xor_text(self):
        ns = SimpleNamespace(
            TitleIs=_macro("T"),
            ValueGt=_macro("V"),
        )
        assert traverse(TitleIs("a") ^ ValueGt(1), ns) == (
            "(((T) AND NOT (V)) OR (NOT (T) AND (V)))"
        )

    def test_xor(self, engine: Engine[Any], items: None):
        crit = ValueGt(8) ^ IdGe(2)
        # a: T^F, b: T^T, c: T^T, d: F^T
        assert titles(engine, crit) == ["a", "d"]

    def test_xor_in_and(self, engine: Engine[Any], items: None):
        crit = (ValueGt(8) ^ IdGe(2)) & TitleIs("d")
        assert titles(engine, crit) == ["d"]

    def test_depth_three(self, engine: Engine[Any], items: None):
        crit = ~(TitleIs("a") | (ValueGt(15) & ~IdGe(3)))
        # a, b исключены; c (id=3) и d остаются
        assert titles(engine, crit) == ["c", "d"]

    def test_missing_macro(self, engine: Engine[Any], items: None):
        with pytest.raises(LookupError, match="NoMacro"):
            titles(engine, TitleIs("a") & NoMacro())

    def test_missing_macro_not_keyerror(self, engine: Engine[Any], items: None):
        with pytest.raises(LookupError) as exc:
            titles(engine, NoMacro())
        assert not isinstance(exc.value, KeyError)

    def test_empty_and(self, engine: Engine[Any], items: None):
        assert titles(engine, And()) == ["a", "b", "c", "d"]

    def test_empty_or(self, engine: Engine[Any], items: None):
        assert titles(engine, Or()) == []

    def test_empty_inside(self, engine: Engine[Any], items: None):
        assert titles(engine, TitleIs("a") & ~Or()) == ["a"]

    def test_returns_true_false(self, engine: Engine[Any], items: None):
        true_: Criteria[Any] = ReturnsTrue()
        false_: Criteria[Any] = ReturnsFalse()
        assert titles(engine, true_ & TitleIs("a")) == ["a"]
        assert titles(engine, false_ | TitleIs("b")) == ["b"]
        assert titles(engine, false_) == []
