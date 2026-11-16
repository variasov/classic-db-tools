---
name: classic-db-tools
description: Справка по API библиотеки classic-db-tools 4.x (classic.db_tools) - Engine, транзакции, запросы из файлов, статические и динамические (Jinja) шаблоны, автобиндинг параметров, трансляция критериев traverse, маппинг Entity/Value/Append/Assign. Загружать перед написанием кода с classic.db_tools. Как библиотека применяется в архитектуре - в python-classic-adapters-sql-db.
license: MIT
---

# classic-db-tools 4.x: API

Только API, сверенный с версией 4.0. Правила применения (раскладка `sql/`, репозитории, где создаётся Engine) - в `python-classic-adapters-sql-db`.

## Engine
```python
import psycopg
from classic.db_tools import Engine, Entity, Value, Append, Assign, Add

engine = Engine(
    psycopg,                                      # драйвер DB API 2.0, позиционно
    lambda: psycopg.connect(conninfo=conninfo),   # фабрика соединений, позиционно
    templates_dirs=[SQL_DIR_PATH],                # str | PathLike | список
)
```
- Первые два аргумента только позиционные (`/` в сигнатуре).
- Создание Engine не открывает соединение: соединения берутся из встроенного потокобезопасного пула при первом запросе. `engine.close()` закрывает простаивающие соединения, движок остаётся рабочим.
- Вне `transaction()`/`conn()` каждый запрос выполняется в автокоммите на своём соединении.
- `templates_dirs` используется и для `query_from`, и для Jinja `{% import %}`/`{% include %}`.

## Транзакции и соединения
```python
with engine.transaction():          # commit при выходе, rollback при исключении
    ...
usecase.run = engine.transaction(usecase.run)   # то же как обёртка функции
with engine.transaction(commit=False): ...      # rollback всегда
with engine.transaction(readonly=True, level='serializable'): ...  # параметры psycopg
with engine.conn(): ...                          # одно соединение без commit/rollback
```
- Вложенный `transaction()` создаёт SAVEPOINT; его параметры должны совпадать с внешней транзакцией (иначе `AssertionError`).
- Все запросы внутри блока идут через одно соединение текущего потока - передавать соединение/курсор в репозитории не нужно. Текущее соединение - `engine.current.conn` (`None` вне блока).

## Запросы
```python
engine.query_from('aggregates/readers/get_by_id.sql')     # путь относительно templates_dirs
engine.query('SELECT %(id)s', static=True)                # из строки; по умолчанию static=False
```
- Файл `.sql` - статический: уходит в драйвер как есть, параметры - `%(name)s`.
- Файл `.sql.tmpl` - динамический: Jinja2-шаблон, собирается при каждом выполнении.
- Другое расширение у `query_from` - `ValueError`.
- Файлы загружаются лениво и кешируются.

### Динамические шаблоны
- Каждое `{{ expr }}` автоматически оборачивается в фильтр `bind`: значение уходит параметром запроса, а не текстом. Явно писать `|bind` не нужно.
- `{{ name | identifier }}` - идентификатор в кавычках (`"col"`); значение - только из белого списка.
- `{{ values | inclause }}` - список для `IN (...)`.
- `{{ text | sqlsafe }}` (или `| safe`) - вставить как есть. Только для доверенного SQL.
- Шаблон проходит через `%`-форматирование драйвера: литеральный `%` пишется `%%` (`ILIKE '%%' || {{ q }} || '%%'`).
- Доступны глобальные функции `traverse` и `contains` (ниже).

### Критерии в SQL: traverse
```sql
{# criteria.sql.tmpl #}
{% macro overdue(criteria) -%}
    l.returned_at IS NULL AND l.due < {{ criteria.on }}
{%- endmacro %}
```
```sql
{% import 'aggregates/loans/criteria.sql.tmpl' as macros %}
SELECT ... FROM library.loans l
WHERE {{ traverse(criteria, macros) | safe }}
```
- **`| safe` обязателен.** Без него результат составного критерия (`&`, `|`, `~`) уходит в драйвер одним строковым параметром, и запрос падает (`invalid input syntax for type boolean`).
- `traverse(criteria, macros)` раскрывает `&`/`|`/`~` в `AND`/`OR`/`NOT` и сам оборачивает каждый узел и лист в скобки - скобки в макросах не нужны.
- Для листа вызывается макрос с именем класса критерия. У критерия из `@criteria` имя класса = имя метода (`Loan.overdue(on=...)` → класс `overdue`), поэтому макрос называется как метод.
- Макрос получает ОДИН аргумент - объект критерия; параметры читаются атрибутами (`criteria.on`) и биндятся автоматически с уникальными именами (один макрос можно использовать в дереве многократно).
- Нет макроса с нужным именем - `LookupError: No macro "<name>"` при выполнении.
- `contains(criteria, 'overdue', 'returned')` - есть ли в дереве критерий с одним из имён (для `{% if %}`).

## Выполнение и результаты
Параметры передаются kwargs или dict (оба можно вместе, kwargs приоритетнее); объект - через `**obj.__dict__`.

| Метод | Возвращает |
|---|---|
| `.execute(**params)` | курсор (см. ниже) |
| `.executemany([...])` | пакетное выполнение |
| `.one(**params)` | первая строка или `None` |
| `.all(**params)` | список строк |
| `.iter(batch=500, **params)` | итератор строк |
| `.scalar(**params)` | первое значение первой строки или `None`; `raising=True` - `IndexError` |
| `.scalars(**params)` | итератор значений первого столбца |
| `.rowcount(**params)` | число затронутых строк |

- Без маппинга строки - кортежи драйвера.
- **`.execute()` вне `transaction()`/`conn()` возвращает `DetachedCursor`**: соединение уже возвращено в пул, доступен только `rowcount`, а `fetchone()` бросает `CursorOutsideScopeError`. Внутри блока - обычный курсор.
- `INSERT ... RETURNING id`: `.scalar(**params)` - работает и в блоке, и вне его. `.execute(...).fetchone()` - только внутри транзакции.
- `.iter()`/`.scalars()` вне блока держат своё соединение до исчерпания - используй `with query.iter() as rows:`.

## Маппинг
```python
.map_to(Reader, Reader=Entity(Reader, 'id')).one(reader_id=1)       # -> Reader | None
.map_to(
    Loan,
    loan=Entity(Loan, 'id', extensions=Append('extension'), period=Assign('period')),
    extension=Entity(Extension, 'id'),
    period=Value(Period),
).all()                                                             # -> list[Loan]
```
- Колонки называются `<prefix>__<field>` (`r.id AS Reader__id`). Регистр не важен: имена приводятся к нижнему.
- `map_to(Cls)` без kwargs - префикс = имя класса, конфигурация - `default_mapping` Engine. С kwargs - kwargs заменяют конфигурацию; первый аргумент задаёт тип результата и префикс (`map_to(Cls, 'prefix', ...)` - явный префикс).
- `Entity(Cls, 'id')` или `Entity(Cls, ('a', 'b'))` - объект с идентификатором; строки с одинаковым id схлопываются в один объект (`all()` сохраняет порядок первого появления).
- `Value(Cls)` - объект-значение, создаётся на каждую строку; все поля `NULL` → `None` (`Value(Cls, False)` - создать всё равно).
- Связи: `Append('prefix')` - `.append` в список (one-to-many), `Add('prefix')` - `.add` в set, `Assign('prefix')` - one-to-one. Если поле `Assign` есть в конструкторе, VO собирается первым и передаётся аргументом - работает с обязательными полями и `frozen` датаклассами.
- Две коллекции объектов-значений в одном JOIN дают дубли - выноси в отдельные запросы. `ORDER BY` по корню рекомендован.
- Пагинация корней с коллекцией: страница корней в CTE (`LIMIT/OFFSET`), дети - JOIN к ней.
- После `map_to` доступны `.one()`, `.all()`, `.iter()`; `.sources()` - текст сгенерированного маппера для отладки.

## Ошибки
- `ConnectionLimitError` (из `classic.db_tools`) - пул исчерпан (`pool_kwargs={'limit': N, 'timeout': s}`).
- `CursorOutsideScopeError` - выборка из `DetachedCursor` вне блока.
- Ошибки драйвера (`psycopg.errors.*`) пробрасываются как есть.
- Ошибка синтаксиса шаблона - `jinja2.TemplateError`.
