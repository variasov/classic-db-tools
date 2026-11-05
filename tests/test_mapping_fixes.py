"""Тесты исправлений маппера (L6): отношения, аннотации, префиксы,
дедупликация корня, колонки без префикса."""
import sqlite3
from dataclasses import dataclass, field
from typing import Any

import pytest

from classic.db_tools import Add, Append, Assign, Engine, Entity, Value
from classic.db_tools.mapping.mapper import Mapper
from classic.db_tools.mapping.params import create_mapping

pytestmark = pytest.mark.usefixtures("data")


@dataclass
class Status:
    title: str = ""


@dataclass
class Task:
    id: int = 0
    title: str = ""
    statuses: list[Any] = field(default_factory=list[Any])
    status: object = None


@dataclass
class Child:
    id: int = 0
    label: str = ""


class FakeCursor:
    def __init__(self, columns, rows):
        self.description = tuple((c,) for c in columns)
        self._rows = list(rows)

    def fetchall(self):
        return self._rows


def _map(mapping, result, columns, rows):
    frozen = create_mapping(**mapping)
    cursor = FakeCursor(columns, rows)
    func = Mapper(frozen).func_for_cursor(cursor, frozen, result)
    return list(func(iter(rows)))


class TestRelationshipTarget:
    def test_assign_accepts_class(self):
        assert Assign(Status).target == "status"

    def test_append_accepts_class(self):
        assert Append(Child).target == "child"

    def test_add_accepts_class(self):
        assert Add(Child).target == "child"

    def test_instance_and_string_still_work(self):
        assert Assign("Status").target == "status"
        assert Assign(Status("x")).target == "status"


class TestUnknownRelationships:
    def test_unknown_relationship_kept(self):
        entity = Entity(Task, "id", extensions=Append("extension"))
        assert entity.relationships["extensions"].target == "extension"

    def test_var_keyword_factory_accepts_unknown(self):
        def factory(id, **kw):  # noqa: A002
            return Task(id=id, title=kw.get("title", ""))

        entity = Entity(factory, "id", extra=Assign("status"))
        assert entity.relationships["extra"].target == "status"

    def test_var_keyword_not_turned_into_relationship(self):
        def factory(id, **kw):  # noqa: A002
            return Task(id=id)

        entity = Entity(factory, "id")
        assert "kw" not in entity.relationships


class TestStringAnnotations:
    def test_future_annotations_auto_relationships(self):
        from tests.future_models import FutureTask

        entity = Entity(FutureTask, "id")
        rel = entity.relationships["statuses"]
        assert isinstance(rel, Append)
        assert rel.target == "futurestatus"


class TestFunctionFactoryAccessor:
    def test_assign_applied_via_attribute(self):
        def make_task(id, title):  # noqa: A002
            return Task(id=id, title=title)

        (task,) = _map(
            {
                "task": Entity(make_task, "id", status=Assign("status")),
                "status": Value(Status),
            },
            "task",
            ("task__id", "task__title", "status__title"),
            [(1, "A", "s1")],
        )
        assert isinstance(task.status, Status)
        assert task.status.title == "s1"


class TestEqualParamsPrefixes:
    def test_two_prefixes_with_equal_entities(self):
        def make_child(id, label):  # noqa: A002
            return Child(id=id, label=label)

        mapping = {
            "c1": Entity(make_child, "id"),
            "c2": Entity(make_child, "id"),
        }
        (obj,) = _map(
            mapping,
            "c1",
            ("c1__id", "c1__label", "c2__id", "c2__label"),
            [(1, "a", 2, "b")],
        )
        assert obj.id == 1


class TestDefaultMappingLowercase:
    def test_default_mapping_keys_lowered(self):
        engine = Engine(
            sqlite3,
            lambda: sqlite3.connect(":memory:"),
            default_mapping={"FlatTask": Entity(Task, "id")},  # type: ignore[arg-type]
        )
        task = (
            engine.query(
                "SELECT 1 AS flattask__id, 'x' AS flattask__title",
                static=True,
            )
            .map_to(Task, "flattask")
            .one()
        )
        assert task is not None
        assert task.id == 1
        assert task.title == "x"


class TestColumnsWithoutPrefix:
    def test_column_without_prefix_skipped(self, engine: Engine[Any]):
        task = (
            engine.query(
                "SELECT 1 AS task__id, 'a' AS task__title, 42 AS cnt",
                static=True,
            )
            .map_to(Task, "task", task=Entity(Task, "id"))
            .one()
        )
        assert task is not None
        assert task.title == "a"


class TestRootDeduplication:
    def test_unsorted_rows_deduplicate_roots(self, engine: Engine[Any]):
        tasks = (
            engine.query(
                "SELECT 1 AS task__id, 'A' AS task__title, "
                "'s1' AS status__title "
                "UNION ALL SELECT 2, 'B', 's2' "
                "UNION ALL SELECT 1, 'A', 's3'",
                static=True,
            )
            .map_to(
                Task,
                "task",
                task=Entity(Task, "id", statuses=Append("status")),
                status=Value(Status),
            )
            .all()
        )
        assert [t.id for t in tasks] == [1, 2]
        assert [s.title for s in tasks[0].statuses] == ["s1", "s3"]
