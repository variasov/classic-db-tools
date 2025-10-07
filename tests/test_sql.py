import os.path
from functools import wraps
from itertools import chain
import time
from typing import Sequence

import pytest
import psycopg


@pytest.fixture()
def conn():
    env = os.environ
    conn = psycopg.connect(f'''
        host={env.get('DB_HOST', 'localhost')}
        port={env.get('DB_HOST', '5432')} 
        dbname={env.get('DB_NAME', 'example')} 
        user={env.get('DB_USER', 'variasov')} 
        password={env.get('DB_PASSWORD', '123')} 
    ''')
    with conn as conn:
        yield conn

width = 10
length = 1000

data = [
    tuple(range(width))
    for i in range(length)
]
data_maps = [
    {
        'field_0': 0,
        'field_1': 1,
        'field_2': 2,
        'field_3': 3,
        'field_4': 4,
        'field_5': 5,
        'field_6': 6,
        'field_7': 7,
        'field_8': 8,
        'field_9': 9,
    } for _ in range(length)
]


def print_time(fn):

    @wraps(fn)
    def wrapper(*args, **kwargs):
        started = time.time()
        result = fn(*args, **kwargs)
        print(f'{time.time() - started}')
        return result

    return wrapper


@print_time
def test_interpolation(conn):
    sql_values = ','.join(
        (
            '({})'.format(
                ','.join(
                    '%s' for __ in range(width)
                )
            )
            for _ in data
        )
    )
    cursor = conn.cursor()
    cursor.execute(
        f'''INSERT INTO example(
            field_0,
            field_1,
            field_2,
            field_3,
            field_4,
            field_5,
            field_6,
            field_7,
            field_8,
            field_9
        ) VALUES {sql_values}''',
        list(chain(*data)),
        binary=True,
    )


@print_time
def test_executemany(conn):
    cursor = conn.cursor()
    cursor.executemany(
        '''
        INSERT INTO example(
            field_0,
            field_1,
            field_2,
            field_3,
            field_4,
            field_5,
            field_6,
            field_7,
            field_8,
            field_9
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''',
        data,
    )


@print_time
def test_unnest(conn):
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO example(
            field_0,
            field_1,
            field_2,
            field_3,
            field_4,
            field_5,
            field_6,
            field_7,
            field_8,
            field_9
        )
        SELECT *
        FROM unnest(
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[]
        )
        ''',
        list(list(val) for val in zip(*data)),
        binary=True,
    )


@print_time
def test_unnest_on_maps(conn):
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO example(
            field_0,
            field_1,
            field_2,
            field_3,
            field_4,
            field_5,
            field_6,
            field_7,
            field_8,
            field_9
        )
        SELECT *
        FROM unnest(
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[],
             %s::int[]
        )
        ''',
        list(
            list(val)
            for val in zip(*(
                dct.values() for dct in data_maps
            ))
        ),
        binary=True,
    )

@print_time
def test_executemany_on_maps(conn):
    cursor = conn.cursor()
    cursor.executemany(
        '''
        INSERT INTO example(
            field_0,
            field_1,
            field_2,
            field_3,
            field_4,
            field_5,
            field_6,
            field_7,
            field_8,
            field_9
        ) VALUES (
            %(field_0)s,
            %(field_1)s,
            %(field_2)s,
            %(field_3)s,
            %(field_4)s,
            %(field_5)s,
            %(field_6)s,
            %(field_7)s,
            %(field_8)s,
            %(field_9)s
        )
        ''',
        data_maps,
    )
