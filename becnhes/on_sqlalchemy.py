from dataclasses import dataclass
import logging.config
from unittest.mock import Mock, patch
import time

from classic.components import factory
from sqlalchemy import (
    create_engine, select,
    MetaData, Table, Column, Text, Integer, ForeignKey
)
from sqlalchemy.orm import sessionmaker, registry, relationship
import psycopg

from conftest import tasks_rows


# App
@dataclass(slots=False)
class Task:
    id: int
    name: str
    statuses: list['TaskStatus'] = factory(list)


@dataclass(slots=False)
class TaskStatus:
    id: int
    status: str



# DB Schema
metadata = MetaData(schema='public')
tasks_table = Table(
    'tasks',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', Text),
)
tasks_statuses_table = Table(
    'task_status',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('task_id', Integer, ForeignKey('tasks.id')),
    Column('status', Text),
)


# Mapping
registry = registry(metadata=metadata)
registry.map_imperatively(
    Task, tasks_table,
    properties=dict(
        statuses=relationship('TaskStatus', uselist=True),
    ),
)
registry.map_imperatively(TaskStatus, tasks_statuses_table)


# Logging conf
logging.config.dictConfig({
    'version': 1,
    'handlers': {
        '': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
        },
    },
    'loggers': {
        'timer': {
            'level': 'INFO',
            'handlers': [''],
        }
    }
})


# @patch("sqlalchemy.engine.Engine.connect")
def test_sqlalchemy_speed():
    print('\n')

    # connect_mock.return_value.__enter__.return_value = connection
    engine = create_engine(
        'postgresql+psycopg://variasov:123@localhost:5432/test',
    )
    session_factory = sessionmaker(bind=engine)

    for index in range(10):
        started_at = time.time()

        session = session_factory()
        results = session.scalars(
            select(Task).join(Task.statuses).limit(5000)
        ).unique().all()

        print(f'Finished in: {time.time() - started_at}')
        print(f'Objects mapped: {len(results)}')

        session.close()
