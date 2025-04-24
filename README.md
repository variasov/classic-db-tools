# Генерация SQL-запросов используя Jinja-шаблон #

Идея библиотеки заключается в попытке работать с SQL-запросами как с шаблонами Jinja.
Вдохновлено [embrace](https://pypi.org/project/embrace/) и 
[jinjasql](https://pypi.org/project/jinjasql/), оттуда же бралась часть кода.

## Установка:
```shell
pip install classic-sql-tools
```

## Quickstart:

```python
from classic.sql_tools import Module
import psycopg

# Модуль - точка входа во все функции библиотеки.
# При инстанцировании запоминает указанный путь,
# дальнейшие обращения будут
queries = Module('path/to/sql/templates/dir')

# Создадим подключение к БД
conn = psycopg.connect('posgresql:///some_db')

# Применим схему:
queries.tasks.ddl(conn)

# Сохранение данных
queries.tasks.save_task(conn, [
    {'title': 'Some Task', 'body': 'Do something'},
    {'title': 'Another Task', 'body': 'Do anything'},
])

# Получение данных
task = queries.tasks.get_by_id(conn, id=1).one()
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
SELECT id, title, body FROM tasks WHERE id = {{ id }};
```

`sql/tasks/save_task.sql`:
```sql
INSERT INTO tasks (title, body) VALUES ({{ title }}, {{ body }});
```

## Возможности


```python
# Класс Module - точка входа во все функции библиотеки.
from classic.sql_tools import Module


# При инстанцировании ему обязательно нужно 
# передать путь до директории с шаблонами.
queries = Module('path/to/sql')

# Затем можно получить шаблон запроса, лежащего,
# например, в `./sql/some_file.sql`:
query = queries.some_file

# Module поддерживает обращение имен директорий в Python.
# То есть объект-запрос можно получить, 
# обратившись к атрибуту Module с названием директории.
# Например, файл, лежащий в `./sql/some_dir/some_file.sql`:
query = queries.some_dir.some_file

#Вложенность может быть любой:
query = queries.some_dir.another_dir.etc.some_file

#Также можно получить объект запроса, 
#напрямую обратившись по его относительному пути:
query = queries.from_file('sql/some_dir/some_file.sql')

# И можно получить объект запроса из строкового литерала:
query = queries.from_str('SELECT id FROM tasks')

# После получения объект запроса можно выполнить 
# с использованием соединения или курсора:
import psycopg

conn = psycopg.connect('posgresql:///some_db')

result = queries.tasks.get_by_id(conn)
# Либо
cursor = conn.cursor()
result = queries.tasks.get_by_id(cursor)

# Также можно выполнить запрос через метод .execute:
query = queries.tasks.get_by_id
query.execute(conn)

# Объект результата нужен для удобного представления 
# результатов запроса. У него есть методы для представления набора строк, 
# единичных строк и единичных значений - 
# .many(), .one() и .scalar() соответственно

# Вернет список кортежей:
result = queries.from_str('SELECT * FROM tasks').execute(conn)
print(result.many())

# Вернет до 100 строк. При повторном вызове вернет следующие 100:
result = queries.from_str('SELECT * FROM tasks').execute(conn)
print(result.many(100))
print(result.many(100))

# Вернет один кортеж или None:
result = queries.from_str(
    'SELECT * FROM tasks WHERE id = 1'
).execute(conn)
print(result.one())

# Вернет один кортеж или исключение ValueError, 
# если в БД ничего не нашлось:
result = queries.from_str('SELECT * FROM tasks WHERE id = 1').execute(conn)
print(result.one(raising=True))

# Вернет один кортеж или None:
result = queries.from_str(
    'SELECT id FROM tasks WHERE id = 1'
).execute(conn).scalar()
print(result.scalar())

# Аналогично .one() вернет один кортеж или 
# исключение ValueError, если в БД ничего не нашлось:
result = queries.from_str('SELECT id FROM tasks WHERE id = 1').execute(conn)
print(result.scalar(raising=True))
```
