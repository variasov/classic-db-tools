from dataclasses import dataclass, field

import pytest

from classic.db_tools import Entity, Value, Append, Assign, Add

pytestmark = pytest.mark.usefixtures('data')


@dataclass
class Item:
    id: int = 0
    title: str = ''
    value: int = 0


@dataclass
class NullableItem:
    id: int = None
    title: str = None


@dataclass
class Parent:
    id: int = 0
    name: str = ''
    children: list = field(default_factory=list)
    child: object = None


@dataclass
class Child:
    id: int = 0
    label: str = ''


@dataclass
class Summary:
    total: int = 0


@dataclass(frozen=True)
class Tag:
    name: str = ''


@dataclass
class ParentWithTags:
    id: int = 0
    name: str = ''
    tags: set = field(default_factory=set)


INSERT_ITEM = 'insert_map_item.sql'
INSERT_PARENT = 'insert_map_parent.sql'
INSERT_CHILD = 'insert_map_child.sql'
INSERT_TAG = 'insert_map_tag.sql'
SELECT_ITEMS = 'select_map_items_as_item.sql'
SELECT_ITEMS_PREFIX = 'select_map_items_as_myitem.sql'
SELECT_ITEMS_KW = 'select_map_items_as_it.sql'
JOIN_PARENTS_CHILDREN = 'join_parents_children.sql'
JOIN_PARENTS_TAGS = 'join_parents_tags.sql'
UNION_ALL_ITEMS = 'select_map_items_union_all.sql'


def _insert_item(engine, title, value):
    engine.query_from(INSERT_ITEM).execute(t=title, v=value)


class TestMappingEntity:

    def test_map_to_one(self, engine):
        _insert_item(engine, 'map_one', 10)
        item = engine.query_from(SELECT_ITEMS).map_to(Item, 'item', item=Entity(Item, 'id')).one()
        assert item is not None
        assert item.title == 'map_one'
        assert item.value == 10

    def test_map_to_all(self, engine):
        _insert_item(engine, 'a', 1)
        _insert_item(engine, 'b', 2)
        items = engine.query_from(SELECT_ITEMS).map_to(Item, 'item', item=Entity(Item, 'id')).all()
        assert len(items) == 2
        assert items[0].title == 'a'
        assert items[1].value == 2

    def test_entity_identity_map_deduplicates(self, engine):
        _insert_item(engine, 'same', 99)
        rows = engine.query_from(UNION_ALL_ITEMS).map_to(
            Item, 'item', item=Entity(Item, 'id'),
        ).all()
        assert len(rows) == 1

    def test_entity_with_custom_prefix(self, engine):
        _insert_item(engine, 'prefix', 7)
        item = engine.query_from(SELECT_ITEMS_PREFIX).map_to(
            Item, 'myitem', myitem=Entity(Item, 'id'),
        ).one()
        assert item is not None
        assert item.title == 'prefix'

    def test_entity_with_any_prefix_in_kwargs(self, engine):
        _insert_item(engine, 'kwmap', 3)
        item = engine.query_from(SELECT_ITEMS_KW).map_to(Item, 'it', it=Entity(Item, 'id')).one()
        assert item is not None
        assert item.title == 'kwmap'

    def test_mapper_iter(self, engine):
        _insert_item(engine, 'a', 1)
        _insert_item(engine, 'b', 2)
        items = list(
            engine.query_from(SELECT_ITEMS).map_to(Item, 'item', item=Entity(Item, 'id')).iter(batch=1),
        )
        assert len(items) == 2

    def test_mapper_iter_without_batch(self, engine):
        _insert_item(engine, 'a', 1)
        _insert_item(engine, 'b', 2)
        items = list(
            engine.query_from(SELECT_ITEMS).map_to(Item, 'item', item=Entity(Item, 'id')).iter(batch=None),
        )
        assert len(items) == 2

    def test_returns_none_when_id_is_null(self, engine):
        items = engine.query(
            'SELECT NULL AS item__id, '
            "'none_title' AS item__title, "
            'NULL AS item__value',
            static=True,
        ).map_to(Item, 'item', item=Entity(Item, 'id')).all()
        assert items == []


class TestMappingValue:

    def test_value_mapping(self, engine):
        s = engine.query(
            'SELECT 42 AS summary__total', static=True,
        ).map_to(
            Summary, 'summary', summary=Value(Summary),
        ).one()
        assert s is not None
        assert s.total == 42

    def test_value_reduce_none_true(self, engine):
        item = engine.query(
            'SELECT NULL AS val__id, NULL AS val__title',
            static=True,
        ).map_to(
            NullableItem, 'val',
            val=Value(NullableItem, True),
        ).one()
        assert item is None

    def test_value_reduce_none_false(self, engine):
        item = engine.query(
            'SELECT NULL AS val__id, NULL AS val__title',
            static=True,
        ).map_to(
            NullableItem, 'val',
            val=Value(NullableItem, False),
        ).one()
        assert item is not None
        assert item.id is None
        assert item.title is None

    def test_value_no_identity_dedup(self, engine):
        rows = engine.query('''
            SELECT 1 AS summary__total
            UNION ALL
            SELECT 2 AS summary__total
        ''', static=True).map_to(
            Summary, 'summary', summary=Value(Summary, False),
        ).all()
        assert len(rows) == 2
        assert rows[0].total == 1
        assert rows[1].total == 2


class TestMappingRelationships:

    def _seed_parent_with_children(self, engine):
        engine.query_from(INSERT_PARENT).execute(i=1, n='p1')
        engine.query_from(INSERT_CHILD).executemany(
            [{'p': 1, 'l': 'c1'}, {'p': 1, 'l': 'c2'}],
        )

    def test_append_relationship(self, engine):
        self._seed_parent_with_children(engine)
        parents = engine.query_from(JOIN_PARENTS_CHILDREN).map_to(
            Parent, 'parent',
            parent=Entity(Parent, 'id', children=Append('child')),
            child=Value(Child),
        ).all()
        assert len(parents) == 1
        assert len(parents[0].children) == 2
        labels = {c.label for c in parents[0].children}
        assert labels == {'c1', 'c2'}

    def test_assign_relationship(self, engine):
        engine.query_from(INSERT_PARENT).execute(i=1, n='p_assign')
        engine.query_from(INSERT_CHILD).execute(p=1, l='c_assign')
        parent = engine.query_from(JOIN_PARENTS_CHILDREN).map_to(
            Parent, 'parent',
            parent=Entity(Parent, 'id', child=Assign('child')),
            child=Value(Child),
        ).one()
        assert parent is not None
        assert parent.child is not None
        assert parent.child.label == 'c_assign'

    def test_append_null_relationship(self, engine):
        engine.query_from(INSERT_PARENT).execute(i=1, n='null_rel')
        parents = engine.query_from(JOIN_PARENTS_CHILDREN).map_to(
            Parent, 'parent',
            parent=Entity(Parent, 'id', children=Append('child')),
            child=Value(Child),
        ).all()
        assert len(parents) == 1
        assert parents[0].name == 'null_rel'
        assert parents[0].children == []

    def test_add_relationship(self, engine):
        engine.query_from(INSERT_PARENT).execute(i=1, n='p_tags')
        engine.query_from(INSERT_TAG).executemany(
            [{'p': 1, 'n': 't1'}, {'p': 1, 'n': 't2'}],
        )
        parents = engine.query_from(JOIN_PARENTS_TAGS).map_to(
            ParentWithTags, 'parent',
            parent=Entity(ParentWithTags, 'id', tags=Add('tag')),
            tag=Value(Tag),
        ).all()
        assert len(parents) == 1
        assert parents[0].tags == {Tag('t1'), Tag('t2')}

    def test_assign_null_relationship(self, engine):
        engine.query_from(INSERT_PARENT).execute(i=1, n='no_child')
        parent = engine.query_from(JOIN_PARENTS_CHILDREN).map_to(
            Parent, 'parent',
            parent=Entity(Parent, 'id', child=Assign('child')),
            child=Value(Child),
        ).one()
        assert parent is not None
        assert parent.child is None


class TestMappingSources:

    def test_sources_returns_source_code(self, engine):
        _insert_item(engine, 'src', 0)
        sources = engine.query_from(SELECT_ITEMS).map_to(Item, 'item', item=Entity(Item, 'id')).sources()
        assert 'mapper_func' in sources

    def test_sources_without_cursor(self, engine):
        _insert_item(engine, 'src2', 1)
        sources = engine.query_from(SELECT_ITEMS).map_to(
            Item, 'item', item=Entity(Item, 'id'),
        ).sources()
        assert 'map_item' in sources
