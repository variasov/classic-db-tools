# Генерация SQL-запросов используя Jinja-шаблон #

Пример использования:

tasks/get_by_id.sql:
```sql
SELECT id, name FROM tasks WHERE id = {{ id }};
```

tasks/get_all.sql:
```sql
SELECT id, name FROM tasks
WHERE
{% if name %} name LIKE {{ name }} AND {% endif %}
TRUE;
```

tasks/save.sql
```sql
INSERT INTO tasks (name, value) VALUES ({{ name }}, {{ value }});
```

Использование
```python
queries = Module(os.path.join(os.path.dirname(__file__), 'sql'))


class ExampleRepo:
    
    def __init__(self, queries: Module, connection: connect):
        self.queries = queries
        self.connection = connection
    
    def get_by_id(self, id: int):
        q = self.queries.from_file('tasks/get_by_id.sql')
        # Записи эквивалентны
        result = q.execute(self.connection, {'id': id}).one()
        result = q.one(self.connection, id=id) # (1, '1')
        
        return result

    def get_many(self, id: int):
        q = self.queries.from_file('tasks/get_all.sql')
        # Записи эквивалентны
        result = q.execute(self.connection, {'name': '1'}).many()
        result = q.many(self.connection, name='1') # [(1, '1'), (2, '2')]
        
        return result
    
    def get_one_or_none(self, id: int):
        q = self.queries.from_file('tasks/get_by_id.sql')
        # Записи эквивалентны
        result = q.execute(self.connection, {'id': id}).one_or_none()
        result = q.one_or_none(self.connection, id=id) # (1, '1') или None
        
        return result
    
    def insert_one(self, name: str, value: str):
        q = self.queries.from_file('tasks/save.sql')

        q.execute(self.connection, {'name': name, 'value': value})
    
    def insert_many(self, name: str, value: str):
        q = self.queries.from_file('tasks/save.sql')

        q.execute(
            self.connection, 
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