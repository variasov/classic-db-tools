from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from classic.criteria import And, Criteria, Or, ReturnsFalse, ReturnsTrue
from classic.db_tools.templates.dynamic.criteria_macro import traverse


@dataclass
class TitleIs(Criteria):
    title: str


@dataclass
class ValueGt(Criteria):
    value: int


@dataclass
class IdGe(Criteria):
    min_id: int


@dataclass
class NoMacro(Criteria):
    pass


# Xor рендерит A и B дважды; на qmark (sqlite) это упирается в L1
# (bind-параметры именуются по тексту выражения).
XOR_QMARK = pytest.mark.xfail(
    reason='L1: повторный рендер одного выражения на qmark', strict=True,
)

ITEMS = 'criteria/items.sql.tmpl'


@pytest.fixture
def items(engine, data):
    for title, value in (('a', 10), ('b', 20), ('c', 30), ('d', 5)):
        engine.query_from('insert_item.sql').execute(t=title, v=value)


def titles(engine, criteria):
    rows = engine.query_from(ITEMS).all(criteria=criteria)
    return [row[0] for row in rows]


class TestTraverse:

    def test_single(self, engine, items):
        assert titles(engine, TitleIs('b')) == ['b']

    def test_and_or_precedence(self, engine, items):
        # Без скобок: title = a AND value > 15 OR id >= 3 -- неверно
        crit = TitleIs('a') & (ValueGt(15) | IdGe(3))
        assert titles(engine, crit) == []
        crit = TitleIs('a') & (ValueGt(5) | IdGe(100))
        assert titles(engine, crit) == ['a']

    def test_or_of_and(self, engine, items):
        crit = (TitleIs('c') | IdGe(100)) & ValueGt(15)
        assert titles(engine, crit) == ['c']

    def test_invert_of_and(self, engine, items):
        crit = ~(ValueGt(8) & IdGe(2))
        # a: not(T and F); b, c: not(T and T); d: not(F and T)
        assert titles(engine, crit) == ['a', 'd']

    def test_text(self):
        ns = SimpleNamespace(
            TitleIs=lambda c: 'T', ValueGt=lambda c: 'V',
        )
        crit = ~(TitleIs('a') & ValueGt(1))
        assert traverse(crit, ns) == 'NOT ((T) AND (V))'
        crit = TitleIs('a') & (ValueGt(1) | ValueGt(2))
        assert traverse(crit, ns) == '(T) AND ((V) OR (V))'

    def test_xor_text(self):
        ns = SimpleNamespace(
            TitleIs=lambda c: 'T', ValueGt=lambda c: 'V',
        )
        assert traverse(TitleIs('a') ^ ValueGt(1), ns) == (
            '(((T) AND NOT (V)) OR (NOT (T) AND (V)))'
        )

    @XOR_QMARK
    def test_xor(self, engine, items):
        crit = ValueGt(8) ^ IdGe(2)
        # a: T^F, b: T^T, c: T^T, d: F^T
        assert titles(engine, crit) == ['a', 'd']

    @XOR_QMARK
    def test_xor_in_and(self, engine, items):
        crit = (ValueGt(8) ^ IdGe(2)) & TitleIs('d')
        assert titles(engine, crit) == ['d']

    def test_depth_three(self, engine, items):
        crit = ~(TitleIs('a') | (ValueGt(15) & ~IdGe(3)))
        # a, b исключены; c (id=3) и d остаются
        assert titles(engine, crit) == ['c', 'd']

    def test_missing_macro(self, engine, items):
        with pytest.raises(LookupError, match='NoMacro'):
            titles(engine, TitleIs('a') & NoMacro())

    def test_missing_macro_not_keyerror(self, engine, items):
        with pytest.raises(LookupError) as exc:
            titles(engine, NoMacro())
        assert not isinstance(exc.value, KeyError)

    def test_empty_and(self, engine, items):
        assert titles(engine, And()) == ['a', 'b', 'c', 'd']

    def test_empty_or(self, engine, items):
        assert titles(engine, Or()) == []

    def test_empty_inside(self, engine, items):
        assert titles(engine, TitleIs('a') & ~Or()) == ['a']

    def test_returns_true_false(self, engine, items):
        assert titles(engine, ReturnsTrue() & TitleIs('a')) == ['a']
        assert titles(engine, ReturnsFalse() | TitleIs('b')) == ['b']
        assert titles(engine, ReturnsFalse()) == []
