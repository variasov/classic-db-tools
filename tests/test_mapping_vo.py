"""Тесты L7: сборка объектов-значений аргументами конструктора.

Assign-отношение, чьё поле присутствует в сигнатуре фабрики, собирается
до вызова фабрики и передаётся в неё аргументом. Это позволяет строить
агрегаты с обязательными полями-VO и frozen dataclass'ы (раньше:
TypeError о недостающем аргументе или FrozenInstanceError при
присваивании после конструирования).
"""

from dataclasses import dataclass, field
from typing import Any

import pytest

from classic.db_tools import Assign, Engine, Entity, Value
from classic.db_tools.mapping.mapper import Mapper
from classic.db_tools.mapping.params import create_mapping

pytestmark = pytest.mark.usefixtures("data")


@dataclass(frozen=True)
class Period:
    start: int = 0
    end: int = 0


@dataclass(frozen=True)
class FrozenLoan:
    id: int
    amount: int
    period: Period


@dataclass(frozen=True)
class FrozenLoanMid:
    id: int
    period: Period
    amount: int


@dataclass(frozen=True)
class FrozenLoanNullable:
    id: int = 0
    period: Period | None = None


@dataclass(frozen=True)
class Group:
    id: int = 0
    title: str = ""


@dataclass(frozen=True)
class Owner:
    id: int
    group: Group | None


@dataclass
class LoanWithPostField:
    id: int = 0
    period: object = field(init=False, default=None)


@dataclass(frozen=True)
class Status:
    title: str = ""


@dataclass(frozen=True)
class LoanWithTwoVO:
    id: int
    period: Period
    status: Status


class LoanKwOnly:
    def __init__(
        self,
        id: int,  # noqa: A002
        amount: int,
        period: Period,
        *,
        note: str = "",
    ) -> None:
        self.id = id
        self.amount = amount
        self.period = period
        self.note = note


class FakeCursor:
    def __init__(self, columns, rows):
        self.description = tuple((c,) for c in columns)
        self._rows = list(rows)
        self.rowcount = len(rows)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchmany(self, size, /):
        return self._rows[:size]

    def execute(self, operation, parameters=None, /):
        return self

    def executemany(self, operation, seq_of_parameters, /):
        return self

    def close(self):
        return None


def _map(mapping, result, columns, rows):
    frozen = create_mapping(**mapping)
    cursor = FakeCursor(columns, rows)
    func = Mapper(frozen).func_for_cursor(cursor, frozen, result)
    return list(func(iter(rows)))


class TestFrozenAggregateVO:
    def test_frozen_aggregate_with_required_vo(self):
        (loan,) = _map(
            {
                "loan": Entity(FrozenLoan, "id", period=Assign("period")),
                "period": Value(Period),
            },
            "loan",
            ("loan__id", "loan__amount", "period__start", "period__end"),
            [(1, 100, 10, 20)],
        )
        assert loan == FrozenLoan(id=1, amount=100, period=Period(10, 20))

    def test_vo_param_in_the_middle_of_signature(self):
        (loan,) = _map(
            {
                "loan": Entity(FrozenLoanMid, "id", period=Assign("period")),
                "period": Value(Period),
            },
            "loan",
            ("loan__id", "loan__amount", "period__start", "period__end"),
            [(1, 100, 10, 20)],
        )
        assert loan == FrozenLoanMid(id=1, period=Period(10, 20), amount=100)

    def test_nullable_vo_is_none(self):
        (loan,) = _map(
            {
                "loan": Entity(
                    FrozenLoanNullable,
                    "id",
                    period=Assign("period"),
                ),
                "period": Value(Period),
            },
            "loan",
            ("loan__id", "period__start", "period__end"),
            [(1, None, None)],
        )
        assert loan.id == 1
        assert loan.period is None

    def test_vo_on_value_root(self):
        rows = _map(
            {
                "loan": Value(FrozenLoan),
                "period": Value(Period),
            },
            "loan",
            ("loan__id", "loan__amount", "period__start", "period__end"),
            [(1, 100, 10, 20), (2, 200, 30, 40)],
        )
        assert rows == [
            FrozenLoan(1, 100, Period(10, 20)),
            FrozenLoan(2, 200, Period(30, 40)),
        ]

    def test_ctor_assign_with_entity_right(self):
        owners = _map(
            {
                "owner": Entity(Owner, "id", group=Assign("group")),
                "group": Entity(Group, "id"),
            },
            "owner",
            ("owner__id", "group__id", "group__title"),
            [(1, None, None), (2, 5, "g5")],
        )
        assert owners == [Owner(1, None), Owner(2, Group(5, "g5"))]

    def test_two_ctor_assigns(self):
        (loan,) = _map(
            {
                "loan": Entity(
                    LoanWithTwoVO,
                    "id",
                    period=Assign("period"),
                    status=Assign("status"),
                ),
                "period": Value(Period),
                "status": Value(Status),
            },
            "loan",
            (
                "loan__id",
                "period__start",
                "period__end",
                "status__title",
            ),
            [(1, 10, 20, "new")],
        )
        assert loan == LoanWithTwoVO(1, Period(10, 20), Status("new"))

    def test_keyword_only_vo_param(self):
        (loan,) = _map(
            {
                "loan": Entity(LoanKwOnly, "id", period=Assign("period")),
                "period": Value(Period),
            },
            "loan",
            ("loan__id", "loan__amount", "period__start", "period__end"),
            [(1, 100, 10, 20)],
        )
        assert loan.id == 1
        assert loan.amount == 100
        assert loan.period == Period(10, 20)
        assert loan.note == ""

    def test_assign_not_in_signature_post_assigned(self):
        (loan,) = _map(
            {
                "loan": Entity(
                    LoanWithPostField,
                    "id",
                    period=Assign("period"),
                ),
                "period": Value(Period),
            },
            "loan",
            ("loan__id", "period__start", "period__end"),
            [(1, 10, 20)],
        )
        assert loan.id == 1
        assert loan.period == Period(10, 20)

    def test_frozen_vo_end_to_end(self, engine: Engine[Any]):
        row = (
            engine.query(
                "SELECT 1 AS loan__id, 100 AS loan__amount, "
                "10 AS period__start, 20 AS period__end",
                static=True,
            )
            .map_to(
                FrozenLoan,
                "loan",
                loan=Entity(FrozenLoan, "id", period=Assign("period")),
                period=Value(Period),
            )
            .one()
        )
        assert row == FrozenLoan(id=1, amount=100, period=Period(10, 20))
