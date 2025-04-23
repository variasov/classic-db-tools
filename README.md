# Генерация SQL-запросов используя Jinja-шаблон #

Идея библиотеки заключается в попытке работать с SQL-запросами как с шаблонами Jinja.
Вдохновлено [embrace](https://pypi.org/project/embrace/) и 
[jinjasql](https://pypi.org/project/jinjasql/), у них же бралась часть кода.

Сначала разместим немного SQL-запросов в файлах
(можно найти в директории test/example):

example/get_by_id.sql:
```sql
SELECT id, name FROM tasks WHERE id = {{ id }};
```

example/get_all.sql:
```sql
SELECT id, name FROM tasks
WHERE
{% if name %} name LIKE {{ name }} AND {% endif %}
TRUE;
```

example/save.sql
```sql
INSERT INTO tasks (name, value) VALUES ({{ name }}, {{ value }});
```

Затем, в Python-code приведем пример класса, использующего модуль с запросами

```python
import os.path
from classic.sql_tools import Module
from psycopg import Connection

queries = Module(os.path.join(os.path.dirname(__file__), 'sql'))


class ExampleRepo:

    def __init__(self, queries: Module, conn: Connection):
        self.queries = queries
        self.conn = conn

    def get_many(self):
        q = self.queries.from_file('tasks/get_all.sql')
        return q.execute(self.conn, name='1').many()  # [(1, '1'), (2, '2')]

    def get_many_another(self):
        q = self.queries.from_file('tasks/get_all.sql')
        # У объекта-запроса есть алиас для execute:
        return q(self.conn, name='1')  # [(1, '1'), (2, '2')]

    def get_one_or_none(self, id: int):
        q = self.queries.from_file('tasks/get_by_id.sql')
        # Вернет None, если не нашлось строки
        return q(self.conn, id=id).one()

    def get_one_or_raise(self, id: int):
        q = self.queries.from_file('tasks/get_by_id.sql')
        # Вызовет ValueError, если не нашлось строки.
        # Метод .scalar() ведет себя так же.
        return q(self.conn, id=id).one(raising=True)

    def insert_one(self, name: str, value: str):
        q = self.queries.from_file('tasks/save.sql')

        q.execute(self.conn, name=name, value=value)

    def insert_many(self, name: str, value: str):
        q = self.queries.from_file('tasks/save.sql')

        q.execute(
            self.conn,
            [
                {'name': '1', 'value': 'value_1'},
                {'name': '2', 'value': 'value_2'},
                {'name': '3', 'value': 'value_3'},
            ],
        )
```

### Использование module

```python
# Использование из файла
queries = Module(os.path.join(os.path.dirname(__file__), 'sql'))
q = queries.from_file('tasks/save.sql')
```
```python
# Использование из строки
q = queries.from_str('SELECT id, name FROM tasks WHERE id = {{ id }};')
```

### Возможности запросов

```python
import quopri

result = q.execute(connection, {'id': 1)

# Тип результата - Result. Он может возвращать:

result.one()  # (1, '1')
result.many()  # [(1, '1'), (2, '2')]
result.one_or_none()  # (1, '1') или None
result.scalar()  # 1
result.cursor()  # курсор для постепенного выполнения запроса

# Так же можно использовать синтаксис:
result = q.one(connection, {'id': 1})
result = q.many(connection, {'id': 1})
result = q.one_or_none(connection, {'id': 1})
result = q.scalar(connection, {'id': 1})
```