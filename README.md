# Classic DB Tools

Идея библиотеки заключается в попытке работать с SQL-запросами как с шаблонами Jinja.
Вдохновлено [embrace](https://pypi.org/project/embrace/) и 
[jinjasql](https://pypi.org/project/jinjasql/), оттуда же бралась часть кода.

## Установка:
```shell
pip install classic-db-tools
```

## Quickstart:

```python
from classic.db_tools import Engine, ConnectionPool
import psycopg

pool = ConnectionPool(psycopg.connect)
engine = Engine('path/to/sql/templates/dir', pool)

# При входе движок займет соединение в пуле,
# на выходе, по дефолту, закоммитит
with engine:
    # Применим схему:
    engine.query_from('tasks/ddl.sql').execute()

    # Сохранение данных
    engine.query_from('tasks/save.sql').executemany([
        {'title': 'Some Task', 'body': 'Do something'},
        {'title': 'Another Task', 'body': 'Do anything'},
    ])

    # Получение данных
    task = engine.query_from('tasks/get_by_id.sql').one(id=1)
    # (1, 'Some Task', 'Do something')
```

В директории sql рядом с .py файлом надо разместить 3 файла
(можно найти в директории test/example):

`sql/tasks/ddl.sql`:
```sql
CREATE TABLE tasks (
    id serial PRIMARY KEY,
    title text,
    body text
);
```

`sql/tasks/get_by_id.sql`:
```sql
SELECT id, title, body FROM tasks WHERE id = %(id)s;
```

`sql/tasks/save.sql`:
```sql
INSERT INTO tasks (title, body) VALUES (%(title)s, %(body)s);
```

## Возможности

### Управление коннектом и транзакциями

### Выполнение из файла и напрямую

### Статические запросы

### Динамические запросы

### Выдача значений

### Маппинг

#### Маппинг на классы

#### Маппинг на словари

#### Кастомные идентификаторы

#### Композитные ключи

### Отложенные операции

### ScopedConnection

### Transaction
