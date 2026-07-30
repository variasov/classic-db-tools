from dataclasses import dataclass, field

import psycopg

from classic.db_tools import Engine, Entity, Value, Append, Assign, Add


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


_ITEMS_TABLE = (
    'CREATE TEMP TABLE _map_items ('
    'id SERIAL PRIMARY KEY, title TEXT, value INT)'
)
_PARENTS_TABLE = (
    'CREATE TEMP TABLE _map_parents ('
    'id INT PRIMARY KEY, name TEXT)'
)
_CHILDREN_TABLE = (
    'CREATE TEMP TABLE _map_children ('
    'id SERIAL PRIMARY KEY, parent_id INT, label TEXT)'
)
_TAGS_TABLE = (
    'CREATE TEMP TABLE _map_tags ('
    'parent_id INT, name TEXT)'
)


class TestMappingEntity:

    def _insert_item(self, engine, title, value):
        engine.query(
            'INSERT INTO _map_items (title, value) VALUES (%(t)s, %(v)s)',
            static=True,
        ).execute(t=title, v=value)

    def test_map_to_one(self, engine):
        mapping = {'item': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            self._insert_item(eng, 'map_one', 10)
            item = eng.query(
                'SELECT id AS item__id, title AS item__title, '
                'value AS item__value FROM _map_items',
                static=True,
            ).map_to(Item).one()
            assert item is not None
            assert item.title == 'map_one'
            assert item.value == 10

    def test_map_to_all(self, engine):
        mapping = {'item': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            self._insert_item(eng, 'a', 1)
            self._insert_item(eng, 'b', 2)
            items = eng.query(
                'SELECT id AS item__id, title AS item__title, '
                'value AS item__value FROM _map_items ORDER BY id',
                static=True,
            ).map_to(Item).all()
            assert len(items) == 2
            assert items[0].title == 'a'
            assert items[1].value == 2

    def test_entity_identity_map(self, engine):
        mapping = {'item': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            self._insert_item(eng, 'same', 99)
            rows = eng.query('''
                SELECT id AS item__id, title AS item__title,
                       value AS item__value FROM _map_items
                UNION ALL
                SELECT id AS item__id, title AS item__title,
                       value AS item__value FROM _map_items
            ''', static=True).map_to(Item).all()
            assert len(rows) == 1

    def test_entity_with_custom_prefix(self, engine):
        mapping = {'myitem': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            self._insert_item(eng, 'prefix', 7)
            item = eng.query(
                'SELECT id AS myitem__id, title AS myitem__title, '
                'value AS myitem__value FROM _map_items',
                static=True,
            ).map_to(Item, 'myitem').one()
            assert item is not None
            assert item.title == 'prefix'

    def test_entity_with_kwargs_mapping(self, engine):
        mapping = {'item': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            self._insert_item(eng, 'kwmap', 3)
            item = eng.query(
                'SELECT id AS it__id, title AS it__title, '
                'value AS it__value FROM _map_items',
                static=True,
            ).map_to(Item, 'it', it=Entity(Item, 'id')).one()
            assert item is not None
            assert item.title == 'kwmap'

    def test_mapper_iter_without_batch(self, engine):
        mapping = {'item': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            self._insert_item(eng, 'a', 1)
            self._insert_item(eng, 'b', 2)
            items = list(eng.query(
                'SELECT id AS item__id, title AS item__title, '
                'value AS item__value FROM _map_items ORDER BY id',
                static=True,
            ).map_to(Item).iter(batch=None))
            assert len(items) == 2


class TestMappingValue:

    def test_value_mapping(self, engine):
        mapping = {'summary': Value(Summary)}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            s = eng.query('SELECT 42 AS summary__total', static=True).map_to(Summary).one()
            assert s is not None
            assert s.total == 42

    def test_value_reduce_none_false(self, engine):
        mapping = {'val': Value(NullableItem, False)}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            item = eng.query(
                'SELECT NULL AS val__id, NULL AS val__title',
                static=True,
            ).map_to(NullableItem, 'val').one()
            assert item is not None
            assert item.id is None
            assert item.title is None


class TestMappingRelationships:

    def _seed_data(self, eng):
        eng.query(_PARENTS_TABLE, static=True).execute()
        eng.query(_CHILDREN_TABLE, static=True).execute()
        eng.query(
            'INSERT INTO _map_parents (id, name) VALUES (1, %(n)s)',
            static=True,
        ).execute(n='p1')
        eng.query(
            'INSERT INTO _map_children (parent_id, label) VALUES (1, %(l)s)',
            static=True,
        ).executemany([{'l': 'c1'}, {'l': 'c2'}])

    def test_append_relationship(self, engine):
        mapping = {
            'parent': Entity(Parent, 'id', children=Append('child')),
            'child': Value(Child),
        }
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            self._seed_data(eng)
            parents = eng.query('''
                SELECT p.id AS parent__id,
                       p.name AS parent__name,
                       c.id AS child__id,
                       c.label AS child__label
                FROM _map_parents p
                LEFT JOIN _map_children c ON c.parent_id = p.id
                ORDER BY p.id, c.id
            ''', static=True).map_to(Parent).all()
            assert len(parents) == 1
            assert len(parents[0].children) == 2

    def test_assign_relationship(self, engine):
        mapping = {
            'parent': Entity(Parent, 'id', child=Assign('child')),
            'child': Value(Child),
        }
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_PARENTS_TABLE, static=True).execute()
            eng.query(_CHILDREN_TABLE, static=True).execute()
            eng.query(
                'INSERT INTO _map_parents (id, name) VALUES (1, %(n)s)',
                static=True,
            ).execute(n='p_assign')
            eng.query(
                'INSERT INTO _map_children (parent_id, label) VALUES (1, %(l)s)',
                static=True,
            ).execute(l='c_assign')
            parent = eng.query('''
                SELECT p.id AS parent__id,
                       p.name AS parent__name,
                       c.id AS child__id,
                       c.label AS child__label
                FROM _map_parents p
                LEFT JOIN _map_children c ON c.parent_id = p.id
            ''', static=True).map_to(Parent).one()
            assert parent is not None
            assert parent.child is not None
            assert parent.child.label == 'c_assign'

    def test_null_relationship(self, engine):
        mapping = {
            'parent': Entity(Parent, 'id', children=Append('child')),
            'child': Value(Child),
        }
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_PARENTS_TABLE, static=True).execute()
            eng.query(_CHILDREN_TABLE, static=True).execute()
            eng.query(
                'INSERT INTO _map_parents (id, name) VALUES (1, %(n)s)',
                static=True,
            ).execute(n='null_rel')
            parents = eng.query('''
                SELECT p.id AS parent__id,
                       p.name AS parent__name,
                       c.id AS child__id,
                       c.label AS child__label
                FROM _map_parents p
                LEFT JOIN _map_children c ON c.parent_id = p.id
            ''', static=True).map_to(Parent).all()
            assert len(parents) == 1
            assert parents[0].name == 'null_rel'
            assert parents[0].children == []

    def test_add_relationship(self, engine):
        mapping = {
            'parent': Entity(ParentWithTags, 'id', tags=Add('tag')),
            'tag': Value(Tag),
        }
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_PARENTS_TABLE, static=True).execute()
            eng.query(_TAGS_TABLE, static=True).execute()
            eng.query(
                'INSERT INTO _map_parents (id, name) VALUES (1, %(n)s)',
                static=True,
            ).execute(n='p_tags')
            eng.query(
                'INSERT INTO _map_tags (parent_id, name) VALUES (1, %(l)s)',
                static=True,
            ).executemany([{'l': 't1'}, {'l': 't2'}])
            parents = eng.query('''
                SELECT p.id AS parent__id,
                       p.name AS parent__name,
                       t.name AS tag__name
                FROM _map_parents p
                JOIN _map_tags t ON t.parent_id = p.id
                ORDER BY t.name
            ''', static=True).map_to(ParentWithTags, 'parent').all()
            assert len(parents) == 1
            assert parents[0].tags == {Tag('t1'), Tag('t2')}


class TestMappingQuerySources:

    def test_sources_returns_source_code(self, engine):
        mapping = {'item': Entity(Item, 'id')}
        eng = Engine(psycopg, default_mapping=mapping)
        with eng.transaction():
            eng.query(_ITEMS_TABLE, static=True).execute()
            eng.query(
                'INSERT INTO _map_items (title, value) VALUES (%(t)s, %(v)s)',
                static=True,
            ).execute(t='src', v=0)
            sources = eng.query(
                'SELECT id AS item__id, title AS item__title, '
                'value AS item__value FROM _map_items',
                static=True,
            ).map_to(Item).sources()
            assert 'mapper_func' in sources