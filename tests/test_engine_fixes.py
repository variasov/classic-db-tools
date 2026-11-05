"""Тесты исправлений L9: неизвестный драйвер, Engine.close(),
слияние dict и kwargs в _prepare, utf-8 статических шаблонов."""

import sqlite3
from types import ModuleType
from typing import Any

import pytest

from classic.db_tools import Engine
from classic.db_tools.pool import ConnectionPool

pytestmark = pytest.mark.usefixtures("data")


def _fake_driver(paramstyle: str = "qmark") -> ModuleType:
    module = ModuleType("fake_db")
    module.connect = lambda: None  # type: ignore[attr-defined]
    module.paramstyle = paramstyle  # type: ignore[attr-defined]
    return module


class TestUnknownDriver:
    def test_engine_raises_clear_error(self):
        with pytest.raises(ValueError, match="no Transaction implementation"):
            Engine(_fake_driver())  # type: ignore[arg-type]

    def test_pool_disables_validation_for_unknown_driver(self):
        pool = ConnectionPool(_fake_driver(), object)  # type: ignore[arg-type]
        assert pool.validate is None
        assert pool.before_release is None


class TestEngineClose:
    def test_close_and_reuse(self, standalone_engine: Engine[Any]):
        engine = standalone_engine
        assert engine.query("SELECT 1", static=True).scalar() == 1
        engine.close()
        # движок остается рабочим: пул открывает новое соединение
        assert engine.query("SELECT 2", static=True).scalar() == 2
        engine.close()


class TestPrepareMerge:
    def test_dict_and_kwargs_merged(self, engine: Engine[Any]):
        row = engine.query(
            "SELECT :a AS x, :b AS y",
            static=True,
        ).one({"a": 1}, b=2)
        assert row == (1, 2)

    def test_kwargs_override_dict(self, engine: Engine[Any]):
        row = engine.query(
            "SELECT :a AS x",
            static=True,
        ).one({"a": 1}, a=5)
        assert row == (5,)

    def test_tuple_params_still_work(self, engine: Engine[Any]):
        row = engine.query(
            "SELECT ? AS x, ? AS y",
            static=True,
        ).one((1, 2))
        assert row == (1, 2)

    def test_mapper_query_accepts_tuple(self, engine: Engine[Any]):
        from dataclasses import dataclass

        from classic.db_tools import Entity

        @dataclass
        class Pair:
            id: int = 0
            title: str = ""

        row = (
            engine.query("SELECT ? AS pair__id, ? AS pair__title", static=True)
            .map_to(Pair, "pair", pair=Entity(Pair, "id"))
            .one((7, "seven"))
        )
        assert row is not None
        assert row.id == 7
        assert row.title == "seven"


class TestStaticTemplateEncoding:
    def test_utf8_template(self, tmp_path, standalone_engine: Engine[Any]):
        (tmp_path / "cyrillic.sql").write_text(
            "SELECT 'кириллица'",
            encoding="utf-8",
        )
        engine = Engine(
            sqlite3,
            lambda: sqlite3.connect(":memory:"),
            templates_dirs=[str(tmp_path)],
        )
        assert engine.query_from("cyrillic.sql").scalar() == "кириллица"
