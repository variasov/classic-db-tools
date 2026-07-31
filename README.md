# Classic DB Tools

Идея библиотеки заключается в попытке работать с SQL-запросами как с шаблонами Jinja.
Вдохновлено [embrace](https://pypi.org/project/embrace/) и 
[jinjasql](https://pypi.org/project/jinjasql/), оттуда же бралась часть кода.

## Установка:
```shell
pip install classic-db-tools
```

```python
from classic.db_tools import Engine
import psycopg

# Создание движка с драйвером БД
engine = Engine(
    psycopg,
    lambda: psycopg.connect(
        host='localhost',
        port='5432',
        dbname='tasks',
        user='test',
        password='test'
    ),
    templates_dirs='path/to/sql/templates/dir'
)

# По дефолту движок работает со встроенным пулом в режим автокоммита

# Создание схемы:
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
(можно найти в директории tests/sql):

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


## Управление подключениями и транзакциями
Библиотека рассчитана на 2 способа управления подключениями: внешний и внутренний.

### Внешнее управление подключениями
Когда библиотека не управляет подключениями напрямую, а управление происходит снаружи
(вручную или во внешнем фреймворке). В этом случае следует создавать курсор вручную
и передавать его в методы Engine через параметр `cursor`:
```python
cursor = some_connection.cursor()
engine.query('SELECT 1').scalar(cursor=cursor)
```

### Внутреннее управление подключениями
Когда объект Engine управляет подключениями самостоятельно. Engine предоставляет
два менеджера контекста:

#### Транзакции (engine.transaction())
Основной способ управления транзакциями. При входе берется соединение из пула,
при выходе по умолчанию производится `.commit()`, в случае ошибок — `.rollback()`.
```python
with engine.transaction():
    engine.query('INSERT INTO ...').execute(id=1, value=1)
    engine.query('SELECT * FROM ...').all()
```

Поведение при ошибках можно изменить параметром `commit`:
```python
with engine.transaction(commit=False):
    # При выходе будет rollback независимо от ошибок
    engine.query('SELECT 1').scalar()
```

#### Соединения без транзакций (engine.conn())
Для случаев, когда не требуется управление транзакциями (например, с autocommit режимом), и когда необходимо удержать коннект для нескольких запросов.
При выходе соединение просто возвращается в пул без commit/rollback:

Как контекстный менеджер:
```python
with engine.conn():
    engine.query('SELECT 1').scalar()
    engine.query('SELECT 2').scalar()
```

Как декоратор:
```python
@engine.conn()
def some_method():
    engine.query('SELECT 1').scalar()
    engine.query('SELECT 1').scalar()
```


## Инициализация Engine

Engine принимает следующие параметры:

```python
from classic.db_tools import Engine, backends
import psycopg

engine = Engine(
    # Драйвер БД (обязательный позиционный параметр)
    psycopg,
    
    # Фабрика подключений (опциональный позиционный параметр)
    lambda: psycopg.connect(...),
    
    # Класс пула подключений (опциональный параметр)
    pool_class=ConnectionPool,
    
    # Параметры для инициализации пула (опциональный параметр)
    pool_kwargs={
        'limit': 10,           # Максимальное количество подключений (0 - без лимита)
        'timeout': 5.0,        # Таймаут ожидания доступного подключения
        'validator': 'auto',   # Валидатор подключений ('auto' или экземпляр ConnectionValidator)
    },
    
    # Директории с SQL-шаблонами
    templates_dirs=['path/to/sql', 'other/sql'],
    
    # Дефолтный маппер для результатов
    default_mapping={
        'task': Entity(Task, 'id'),
        'status': Value(Status),
    },
    
    # Логгер
    logger=logging.getLogger('my-logger'),
    
    # Считать строковые запросы статическими по умолчанию
    str_templates_static_by_default=False,
    
    # Символ для обрамления идентификаторов БД
    identifier_quote_char='"',  # '"' для PostgreSQL, '`' для MySQL
)
```

### Параметры ConnectionPool

Пул подключений поддерживает следующие параметры через `pool_kwargs`:

- **limit** (int, default=0): Максимальное количество одновременных подключений.
  0 означает без ограничений.

- **timeout** (float, default=5.0): Время ожидания в секундах до выброса `ConnectionLimitError`
  если нет свободного подключения.

- **validator** (str or ConnectionValidator, default='auto'): Валидатор подключений.
  'auto' автоматически выбирает валидатор для драйвера.
  Поддерживаемые драйверы: psycopg, psycopg2, pymysql, mysqldb, pymssql, oracledb, cx_oracle.


## Поддерживаемые драйверы БД

Classic DB Tools поддерживает следующие драйверы:

| Драйвер | Модуль | Установка |
|---------|--------|-----------|
| PostgreSQL (новый) | `psycopg` | `pip install psycopg[binary]` |
| PostgreSQL (старый) | `psycopg2` | `pip install psycopg2-binary` |
| MySQL | `pymysql` | `pip install pymysql` |
| MySQL (С расширениями) | `mysqldb` | `pip install mysqlclient` |
| MS SQL Server | `pymssql` | `pip install pymssql` |
| Oracle (новый) | `oracledb` | `pip install oracledb` |
| Oracle (старый) | `cx_oracle` | `pip install cx_Oracle` |
| SQLite | `sqlite3` | Встроен в стандартную библиотеку |

Для каждого драйвера есть встроенный валидатор подключений и обработчик транзакций.
При использовании неизвестного драйвера валидация отключается (параметр `validator=None`).

Пример для различных драйверов:
```python
import psycopg
import pymysql

# PostgreSQL с psycopg
pg_engine = Engine(
    psycopg,
    lambda: psycopg.connect(host='localhost', dbname='mydb'),
    templates_dirs='sql',
)

# MySQL с pymysql
mysql_engine = Engine(
    pymysql,
    lambda: pymysql.connect(host='localhost', user='root', database='mydb'),
    templates_dirs='sql',
)
```


## Запросы
Объект Engine дает 2 способа задать запрос - из файла, и напрямую.
Также запросы, могут быть статическими или динамическими.


#### Запросы в файлах (.query_from)
Для работы с запросами в файлах существует метод .query_from:
```python
query = engine.query_from('path/to/some/file.sql')
query.execute()
```

Метод .query_from ищет файл относительно каждого пути,
переданного в параметр template_paths конструктора Engine, в том же порядке,
в каком пути были переданы, до первого раза, когда файл будет найден.

Также каждый взятый зарос заносится в кеш. Это значит, что загрузка запроса
из файла будет происходить лишь единожды.

Также .query_from является ленивым, то есть реальная загрузка запроса
из файла произойдет уже при исполнении запроса.


### Запросы, формируемые в коде (.query)
В случае, когда запрос формируется динамически в python-коде, можно использовать
метод .query.

```python
query = engine.query('SELECT 1')
query.execute()
```

### Статические запросы
Статические запросы передаются в драйвер при исполнении как есть,
без каких-либо преобразований.

Метод .query_from считает статическими запросами файлы, названия которых
заканчиваются на `.sql`.

Чтобы создать статический запрос через метод .query, нужно передать параметр
static=True:
```python
query = engine.query('SELECT {{ value }}', static=True)
query.execute(value=1)
```

По умолчанию Engine считает, что запросы, передаваемые в .query - динамические.
Можно изменить это поведение, установив нужное значение
в параметр str_templates_static_by_default конструктора Engine.

Статические запросы очень легковесны, потому рекомендуется по умолчанию 
выражать запросы статично везде, где это возможно.


### Динамические запросы
Динамические запросы, в отличие от статических, являются шаблонами Jinja.
При исполнении шаблон запроса будет собран, результат передан драйверу.

Метод .query_from считает динамическими запросами файлы, названия которых
заканчиваются на `.sql.tmpl`.

Чтобы создать статический запрос через метод .query, нужно передать параметр
static=False:
```python
query = engine.query('SELECT {{ value }}', static=False)
query.execute(value=1)
```

По умолчанию Engine считает, что запросы, передаваемые в .query - динамические.
Можно изменить это поведение, установив нужное значение
в параметр str_templates_static_by_default конструктора Engine.


#### Сборка шаблонов
В целом, при сборке доступны все возможности Jinja, но есть и особенность.

Каждый placeholder Jinja оборачивается в фильтр bind.
Следовательно, эти 2 примера кода эквивалентны:
```jinja
SELECT {{ value }}
```
и
```jinja
SELECT {{ value|bind }}
```

Фильтр bind вставляет вместо реального значения плейсхолдер, подходящий для
драйвера, с которым идет работа.
Шаблон из предыдущего примера, в случае применения psycopg,
будет скомпилирован в:
```sql
SELECT %(value)s
```

Таким образом, устраняется поле для SQL-инъекций, но, в то же время, 
утяжеляется сборка запроса.

Если необходимо отрендерить значение 'как есть', 
например, если оно было получено из безопасного источника,
можно применить фильтр safe:
```jinja
SELECT {{ value|sqlsafe }}
```

Также, для случая, когда необходимо передавать названия объектов БД
(схем, таблиц, столбцов и прочих), есть фильтр `identifier`:
```jinja
SELECT * FROM {{ table|identifier }}
```

В разных БД идентификаторы выделяются разными знаками препинания.
К примеру, Postgres использует двойные кавычки:
```sql
SELECT * FROM experiments."some_table"
```
MS SQL Server использует обратные кавычки:
```sql
SELECT * FROM [public].[some_table]
```

Изменить это можно в параметре identifier_quote_char 
конструктора Engine. По умолчанию используются двойные кавычки.


## Выдача значений
Объект Query предоставляет несколько способов вернуть результаты запроса.
По дефолту запрос возвращает наружу то, что возвращает драйвер.
Для изменения типа можно использовать маппинг (см. раздел о маппинге).

### Методы получения результатов

#### .all()
Возвращает список всех значений результатов, используя `.fetchall()`:
```python   
for row in engine.query(
    'SELECT * FROM some_table'
).all():
    print(row)
```

#### .iter()
Возвращает итератор по результатам, буферизуя их батчами.
Использует `.fetchmany()` для эффективной работы с большими наборами данных.
Размер батча задается параметром `batch` (по умолчанию 500):
```python
for row in engine.query(
    'SELECT * FROM some_table'
).iter(batch=100):
    print(row)
```

#### .one()
Возвращает первое значение или None, используя `.fetchone()`:
```python
row = engine.query(
    'SELECT * FROM some_table ' 
    'WHERE id = %(id)s'
).one(id=1)
print(row)
```

#### .scalar()
Возвращает первое значение первого столбца или None.
Удобен для запросов вроде `SELECT COUNT(*)`:
```python
name = engine.query(
    'SELECT name FROM some_table '
    'WHERE id = %(id)s'
).scalar(id=1)
print(name)
```

При `raising=True` выбросит `IndexError` вместо возврата None, когда результат пуст:
```python
name = engine.query(
    'SELECT name FROM some_table WHERE id = %(id)s'
).scalar(id=999, raising=True)  # IndexError если строк нет
```

#### .rowcount()
Возвращает количество обработанных строк из курсора.
Удобен для логгирования операций INSERT/UPDATE/DELETE:
```python
rowcount = engine.query(
    'DELETE FROM some_table'
).rowcount()
print(f'Удалено {rowcount} строк')
```

#### .scalars()
Возвращает итератор по значениям первого столбца каждой строки.
Удобен для получения списка значений из одного столбца.
Размер батча задается параметром `batch` (по умолчанию 500):
```python
for name in engine.query(
    'SELECT name FROM some_table'
).scalars(batch=100):
    print(name)
```

#### .execute()
Возвращает курсор после выполнения запроса.
Полезен когда требуется работа с курсором вручную:
```python
cursor = engine.query('SELECT * FROM some_table').execute()
# работа с курсором напрямую
```

#### .executemany()
Для множественного исполнения запроса (пакетные операции):
```python
engine.query(
    'INSERT INTO some_table(id, value) VALUES (%(id)s, %(value)s)'
).executemany([
    {'id': 1, 'value': 'a'},
    {'id': 2, 'value': 'b'},
])
```

Также можно передавать объекты с `__dict__` — они будут преобразованы в словари:
```python
from dataclasses import dataclass

@dataclass
class TaskData:
    title: str
    body: str

engine.query('INSERT INTO tasks (title, body) VALUES (%(title)s, %(body)s)'
).executemany([
    TaskData(title='Task 1', body='Do something'),
    TaskData(title='Task 2', body='Do anything'),
])
```

Поведение отличается для статических и динамических запросов:
- **Статический**: использует `.executemany()` драйвера один раз со всеми параметрами (быстро)
- **Динамический**: собирает и выполняет запрос для каждого набора параметров (медленнее)

### Передача параметров

Все методы принимают параметры одинаково. Есть несколько способов:

#### По ключевым аргументам:
```python
engine.query(
    'INSERT INTO some_table(id, value) VALUES (%(id)s, %(value)s)'
).execute(id=1, value=1)
```

#### Через словарь:
```python
some_obj = {'id': 1, 'value': 1}
engine.query(
    'INSERT INTO some_table(id, value) VALUES (%(id)s, %(value)s)'
).execute(some_obj)
```

#### Объединение словаря и ключевых аргументов:
```python
some_obj = {'id': 1, 'value': 1}
engine.query(
    'INSERT INTO some_table(id, value) VALUES (%(id)s, %(value)s)'
).execute(some_obj, value=2)  # value=2 переопределит значение из словаря
```

### Использование с внешним курсором

Каждому методу можно передать внешний курсор параметром `cursor`,
отвязав исполнение от управления подключениями Engine:
```python
cursor = my_connection.cursor()
engine.query(
    'INSERT INTO some_table(id, value) VALUES (%(id)s, %(value)s)'
).execute(id=1, value=1, cursor=cursor)
```


## Логирование и отладка

Engine поддерживает логирование через стандартный модуль `logging`:

```python
import logging

# Включить логирование на уровне DEBUG для видимости всех операций
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('classic-db-tools')

engine = Engine(
    psycopg,
    lambda: psycopg.connect(...),
    templates_dirs='sql',
    logger=logger,  # Передать логгер в Engine
)

# Теперь все операции будут залогированы
with engine.transaction():
    engine.query('SELECT 1').scalar()
```

Библиотека логирует следующие события:
- Загрузку шаблонов из файлов
- Ошибки валидации подключений
- Проблемы с пулом подключений

### Получение скомпилированного запроса

Для отладки динамических запросов можно получить сгенерированный SQL через метод `.sources()`:

```python
query = engine.query(
    'SELECT * FROM tasks WHERE status = {{ status }}'
).map_to(Task)

# Получить текст скомпилированной функции для отладки
print(query.sources())
```


## Динамические фильтры (Criteria)

Для удобного создания сложных WHERE-условий библиотека интегрирована с 
[classic-criteria](https://pypi.org/project/classic-criteria/). Это опциональная 
зависимость, которая позволяет строить фильтры программно.

Сначала установите пакет:
```shell
pip install classic-db-tools[criteria]
```

Затем используйте `traverse` и `contains` макросы в Jinja шаблонах:

```python
from classic.criteria import And, Eq, Gt

# Создать критерии в Python
criteria = And(
    Eq('status', 'active'),
    Gt('created_at', '2024-01-01')
)

# Использовать в запросе
tasks = engine.query('''
    SELECT * FROM tasks
    {% if filters and contains(filters, 'Eq', 'Gt') %}
    WHERE {{ traverse(filters, translators) }}
    {% endif %}
''').all(filters=criteria, translators=my_translators)
```

Функция `traverse` преобразует объекты criteria в SQL WHERE-условие,
функция `contains` проверяет наличие определённых типов фильтров.


## Обработка ошибок

### ConnectionLimitError
Выбрасывается когда пул подключений исчерпан и превышен таймаут ожидания свободного подключения:

```python
from classic.db_tools import Engine, ConnectionPool
from classic.db_tools.pool import ConnectionLimitError

engine = Engine(
    psycopg,
    lambda: psycopg.connect(...),
    pool_kwargs={
        'limit': 5,      # Максимум 5 одновременных подключений
        'timeout': 2.0,  # Ждать 2 секунды, затем ошибка
    },
)

try:
    with engine.transaction():
        # Если все 5 подключений заняты и новое не появилось за 2 сек
        engine.query('SELECT 1').scalar()
except ConnectionLimitError:
    print('Пул подключений полон, подождите и повторите')
```

### Ошибки валидации подключений
Если подключение потеряно или неисправно, валидатор подключения пытается восстановить его.
После нескольких неудачных попыток валидации происходит переподключение:

```python
# Валидатор автоматически проверяет подключение при получении из пула
# Если проверка не пройдена, подключение отбрасывается и создается новое
engine = Engine(
    psycopg,
    lambda: psycopg.connect(...),
    pool_kwargs={'validator': 'auto'},  # Автоматический валидатор для драйвера
)
```

### Ошибки шаблонов Jinja
При ошибках в синтаксисе Jinja шаблона будет выброшено исключение `jinja2.TemplateError`:

```python
try:
    # Синтаксическая ошибка в шаблоне
    query = engine.query('SELECT * FROM {{ invalid | unknown_filter }}')
    query.scalar()
except Exception as e:
    print(f'Ошибка шаблона: {e}')
```


## Маппинг
По умолчанию данные из запроса возвращаются в виде кортежей. Есть возможность
вернуть данные в виде объектов, и даже иерархий объектов.

Для примера предположим, что у нас есть класс Task и таблица applications:
```python
from dataclasses import dataclass

@dataclass
class Task:
    id: int
    name: str
    description: str
```

```sql
CREATE TABLE applications(
    pk      integer PRIMARY KEY,
    title   varchar,
    content varchar 
)
```

В примере названия таблицы и столбцов намеренно расходятся с названиями
класса и полей, чтобы продемонстрировать отсутствие каких-либо автоматических
привязок к именам, все делается "вручную".

Чтобы смаппить данные, необходимо указать в запросе label для каждого столбца,
состоящий из префикса и названия поля, соединенные через _:
```SQL
SELECT
    pk      AS SomeObj__id,
    title   AS SomeObj__name,
    content AS SomeObj__description
FROM some_table;
```

Нужно помнить, что SQL - регистронезависим, поэтому нет разницы,
в каком регистре писать префиксы и поля.
Библиотека внутри все имена переведет в нижний регистр.

Затем нужно объявить маппинг и сделать запрос:
```python
from classic.db_tools import Engine, Entity
import psycopg

mapping = dict(
    task=Entity(Task, 'id'),
)

engine = Engine(
    psycopg,
    lambda: psycopg.connect(host='localhost', dbname='mydb'),
    templates_dirs='sql',
    default_mapping=mapping,
)

with engine.transaction():
    task = engine.query_from('example_select.sql').map_to(Task).one(id=1)
    print(task)
```

Названия полей маппера, подаваемые в словарь mapping, должны соответствовать
префиксам из запроса. Значения, содержащиеся в mapping - это параметры 
маппера, они могут быть Entity и Value. В любом случае, первым аргументов 
подается класс, на который осуществляется маппинг.

Entity применяется для объектов-сущностей. Это такие объекты в предметной
области, которые имеют свой идентификатор, и друг от друга отличаются 
по номеру, поэтому Entity вторым аргументов требует указать названия полей,
участвующих в идентификаторе, в виде кортежа строк, либо одно название в виде 
строки, если идентификатор состоит из одного поля.

Объекты-сущности при маппинге сопоставляются через идентификатор,
поэтому если запрос выдает несколько строк с одним и тем же идентификатором,
будет инстанцирован только один объект-сущность с таким идентификатором.

Value применяется для объектов-значений. Это такие объекты в предметной области,
которые не имеют идентификатора, и различаются по полному составу полей.
Как правило, это подчиненные, дочерние по отношению к каким-либо сущностям 
объекты. Вторым неименованным аргументом можно подать bool

Объекты-значения при маппинге не сопоставляются друг с другом вообще, маппер
просто инстанцирует такие классы каждый раз, когда соответствующие строки
встречаются в полученном наборе данных.

Также бывает случай, когда во все поля сущности приходит NULL из БД.
Эта ситуация двоякая - с одной стороны, во многих случаях нужно вернуть None
вместо объекта, с другой - бывают случаи, когда все поля объекта содержат
None, и объект при этом "легален" и имеет смысл для бизнеса. По умолчанию
маппер сокращает до None такие объекты, но это поведение можно изменить, указав
вторым неименованным аргументом False:
```python
mapper = dict(
    some_val=Value(SomeObj, False)
)
```

### map_to

Метод map_to имеет 3 группы аргументов - result, prefix и keyword-аргументы **params.
Result и prefix связаны друг с другом.

Если подается только result (желаемый класс), то в качестве префикса будет
использоваться название класса, а класс в result будет использован 
для аннотаций типов.
```python
obj = engine.query_from('test.sql').map_to(Task).one()
# obj - Task с точки зрения typing
```


Если подается prefix, то он будет использован в качестве префикса,
result будет использован для аннотаций типов:
```python
obj = engine.query_from('test.sql').map_to(Task, 'task').one()
# obj - Task с точки зрения typing
```


Результат без конкретного класса:
```python
obj = engine.query_from('test.sql').map_to(object, 'task').one()
# obj - object с точки зрения typing
```

Третий вариант - передать маппинг через keyword-аргументы. В этом случае
можно указать только префикс или класс:
```python
# Маппинг через kwargs
engine.query(sql).map_to(
    Task,
    task=Entity(Task, 'id'),
    status=Value(Status),
).all()

# Маппинг через kwargs с кастомным префиксом
engine.query(sql).map_to(
    Task, 'custom',
    custom=Entity(Task, 'id'),
).all()
```

Если не указать ни то, ни другое, маппер выкинет ошибку, так как он не понимает,
объекты с каким префиксом он должен вернуть в ответ.

Если подан **params (keyword-аргументы), они будут использованы как конфигурация маппера
вместо дефолтной. Третий аргумент - маппер (устаревший способ, используйте **params) -
также поддерживается для обратной совместимости.

Дефолтный маппер задается в конструкторе Engine.


### Relationships

Маппер не будет полноценным без возможности управлять отношениями между
объектами, поэтому Entity и Value принимают keyword-аргументы.
Названия аргументов - это названия полей в классе, участвующие в отношении.
Значения - объекты Assign, Append и Add, принимающие единственный аргумент
- префикс объекта, участвующего в отношении.

Append используется для обработки списков, реализуя OneToMany. При использовании
Append маппер будет использовать метод .append() у указанного свойства:
```python
from dataclasses import dataclass, field

from classic.db_tools import Entity, Value, Append


@dataclass
class Status:
    title: str


@dataclass
class Task:
    id: int
    title: str
    statuses: list[Status] = field(default_factory=list)

    
mapping = dict(
    task=Entity(Task, 'id', statuses=Append('status')),
    status=Value(Status)
)

pool = ConnectionPool(psycopg, lambda: psycopg.connect(...))
engine = Engine(psycopg, default_mapping=mapping)

with engine.transaction():
    tasks = engine.query('''
    SELECT
        tasks.id AS task__id,
        tasks.id AS task__title,
        statuses.title AS status__title
    FROM tasks
    JOIN statuses ON statuses.task_id = tasks.id
    ''').map_to(Task).all()

print(tasks)
# [
#     Task(id=1, title='example', statuses=[
#         Status('new'), 
#         Status('completed'),
#     ]),
# ]
```

Add очень похож на Append, только используется для обработки множеств.
При использовании Add маппер будет использовать метод .add()
у указанного свойства.

Assign используется для присвоения объекта указанному свойству,
реализуя OneToOne. 

Пример:
```python
from dataclasses import dataclass
from classic.db_tools import Engine, Entity, Value, Assign
import psycopg


@dataclass
class Status:
    title: str


@dataclass
class Task:
    id: int
    title: str
    status: Status

    
mapping = dict(
    task=Entity(Task, 'id', status=Assign(Status)),
    status=Value(Status)
)

engine = Engine(psycopg, lambda: psycopg.connect(...), default_mapping=mapping)

with engine.transaction():
    tasks = engine.query('''
    SELECT
        tasks.id AS task__id,
        tasks.id AS task__title,
        statuses.title AS status__title
    FROM tasks
    JOIN statuses ON statuses.task_id = tasks.id
    ''').map_to(Task).all()

print(tasks)
# [
#     Task(id=1, title='example', status=Status('new')]),
# ]
```

Также можно указывать в отношениях классы, в таком случае имена классов 
будут считаться искомыми префиксами.

### inspect

Также маппер умеет разбирать аннотации типов у подаваемых классов,
и автоматически догадываться об отношениях, поэтому маппер из примера выше
можно сократить до:
```python
mapper = dict(
    task=Entity(Task, 'id'),
    status=Value(Status)
)
```

Указанные вручную отношения имеют приоритет над автоматически распознанными.
Так можно кастомизировать поведение маппера.

### Компиляция
Под капотом библиотека компилирует функцию-маппер с учетом полученного курсора,
и производит кеширование, так как компиляция занимает время.
Ключ кеша зависит от маппера, запрашиваемого префикса и названий
и порядка столбцов в запросе.

Для облегчения отладки объект запроса с назначенным маппером имеет метод 
.sources(), возвращающий текст скомпилированный функции.

## Лучшие практики и рекомендации

### 1. Используйте статические запросы когда возможно
Статические запросы (`.sql` файлы) выполняются быстрее, так как не требуют
обработки Jinja шаблона:

```python
# ✅ Хорошо - статический запрос
engine.query_from('simple_select.sql').all()

# ⚠️ Медленнее - динамический запрос
engine.query_from('complex_template.sql.tmpl').all()
```

### 2. Кешируйте мапперы
Мапперы компилируются и кешируются. При использовании одинаковых маппинга
для разных запросов, рекомендуется передавать его в конструктор Engine
как `default_mapping`:

```python
# ✅ Хорошо - маппер в конструкторе, переиспользуется
engine = Engine(
    psycopg,
    factory,
    default_mapping={'task': Entity(Task, 'id')},
)

tasks = engine.query_from('get_tasks.sql').map_to(Task).all()
```

### 3. Управляйте пулом подключений правильно
- Установите подходящий `limit` - слишком большой лимит пустит ресурсы впустую,
  слишком малый вызовет `ConnectionLimitError`
- Используйте `validator='auto'` для автоматической валидации подключений
- Настройте `timeout` в зависимости от нагрузки системы

```python
pool_kwargs = {
    'limit': 10,          # Для типичной веб-апликации
    'timeout': 5.0,       # 5 секунд для ожидания соединения
    'validator': 'auto',  # Валидируем каждое подключение
}
```

### 4. Используйте транзакции для групп операций
Группируйте связанные операции в одну транзакцию для обеспечения консистентности:

```python
# ✅ Хорошо - атомарная операция
with engine.transaction():
    task = engine.query_from('create_task.sql').execute(title='New Task')
    engine.query_from('assign_to_user.sql').execute(task_id=task.id, user_id=user_id)
    # Коммитится только если обе операции успешны
```

### 5. Логируйте операции
Всегда передавайте логгер для отладки и мониторинга:

```python
import logging

logger = logging.getLogger(__name__)
engine = Engine(
    psycopg,
    factory,
    logger=logger,
)
```

### 6. Используйте параметризованные запросы
Никогда не подставляйте значения напрямую в SQL строку:

```python
# ❌ Небезопасно - уязвимо к SQL инъекциям
engine.query(f'SELECT * FROM users WHERE name = {name}').all()

# ✅ Безопасно - параметризованный запрос
engine.query('SELECT * FROM users WHERE name = %(name)s').all(name=name)
```

### 7. Отделите маппирование результатов от логики
Используйте маппинг на уровне запроса, а не в обработчике результатов:

```python
# ✅ Хорошо - маппинг в запросе
tasks = engine.query_from('get_tasks.sql').map_to(Task).all()
for task in tasks:
    print(task.title)  # task - это объект Task
```


## Производительность

### Кеширование
- Шаблоны SQL кешируются после первой загрузки из файла
- Мапперы компилируются и кешируются на основе маппинга и структуры результата
- Для сброса кеша пересоздайте объект Engine

### Батчирование
Для больших наборов данных используйте `.iter()` вместо `.all()`:

```python
# ✅ Эффективно для больших наборов
for row in engine.query('SELECT * FROM large_table').iter(batch=1000):
    process(row)

# ❌ Может привести к нехватке памяти
rows = engine.query('SELECT * FROM large_table').all()
for row in rows:
    process(row)
```

### Статические vs Динамические запросы
Сложность обработки:
- **Статические**: O(1) - просто передаются драйверу
- **Динамические**: O(n) - требуют обработки Jinja шаблона и подстановки параметров


## Лицензия
Проект распространяется под лицензией Apache License 2.0. 
Некоторые части кода (ConnectionPool, validators) взяты из других проектов 
с соответствующими лицензиями.
