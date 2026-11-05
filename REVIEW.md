# Ревью classic-db-tools 3.3.0

Ревью от 2026-11-04 по ветке `main` (`2232a45`). Код совпадает с версией 3.3.0 на PyPI.
Проверка: чтение кода, пробы на sqlite3 и рендер шаблонов без БД, pyright strict.
PostgreSQL во время ревью был недоступен: всё, что требует живого PG, помечено «не проверено».
Пути ниже — относительно `src/classic/db_tools/`.

## Замечания к коду

1. **Ошибки `commit()`/`rollback()` теряются.** [высокая]
   - Где: `transaction.py:115-123` — `return False` внутри `finally` гасит любое исключение (на 3.14 ещё и `SyntaxWarning`).
   - Последствие (проверено на sqlite с DEFERRABLE FK): commit падает, `with engine.transaction()` завершается без исключения, в таблице 0 строк.

2. **Коллизии имён bind-параметров.** [высокая]
   - Где: `templates/dynamic/renderer.py:20-25` (нумерация закомментирована, `# TODO`), `:60-70`, `templates/dynamic/extension.py:11-24`.
   - Имя параметра — текст выражения. `of_reader(1) | of_reader(2)` (макрос с `{{ criteria.reader_id }}`) даёт
     `{'criteria.reader_id': 2}` для обоих условий (проверено).
   - Повтор переменной (`{{ a }}, {{ a }}`, цикл) в qmark/format/numeric даёт неверное число параметров.
   - Два `inclause` в одном запросе затирают друг друга (`inclause_0`…) — тихо неверные значения.
   - Пустой список → `IN ()`. Выражения без имени все получают `bind#0`.

3. **`traverse` не ставит скобки.** [высокая]
   - Где: `templates/dynamic/criteria_macro.py:9-25`.
   - `returned & (of_reader(1) | of_reader(2))` → `A AND (r=1) OR (r=2)`; `~(A & B)` → `NOT A AND B` (проверено рендером).
   - `XOR` нет в PostgreSQL и SQLite. Неизвестный макрос — голый `KeyError` (`:27`).

4. **Курсор/генератор используется после возврата соединения в пул.** [высокая]
   - Где: `query.py:88-89`, `186-187` (`iter`), `309-310` (`scalars`), `mapping/query.py:122-123`.
   - `iter`/`scalars` — генераторы: SQL выполняется после выхода из `ConnectionScope`.
   - Проба: пока генератор жив, `engine.conn()` выдаёт то же соединение. `execute()` вне транзакции возвращает курсор
     соединения, на котором уже сделаны rollback и `SELECT 1`. В многопоточном режиме соединение может забрать другой поток.

5. **psycopg3: savepoint как строковый литерал.** [высокая, не проверено на PG]
   - Где: `backends/psycopg.py:69-86` — `SQL('SAVEPOINT {}').format(name)` со строкой даёт `SAVEPOINT 'savepoint_1'`;
     нужен `sql.Identifier`. Тестов на вложенные транзакции psycopg нет.

6. **Явный `| bind('name')` без `| sqlsafe` подставляется дважды.** [средняя]
   - Где: `templates/dynamic/extension.py:56-59` — AutoBind проверяет только последний токен-имя.
   - Проба: `{{ x | bind('p1') }}` → `%(x)s` с `{'p1': 5, 'x': '%(p1)s'}`. Голый `| bind` без имени — `TypeError` (`renderer.py:48`).

7. **Строковые аннотации роняют `Entity`/`Value`.** [высокая]
   - Где: `mapping/params.py:65-70` — `annot.__name__` у `str`.
   - Последствие (проверено): dataclass с `from __future__ import annotations` → `AttributeError`.

8. **Отношения, которых нет в сигнатуре фабрики, молча теряются.** [высокая]
   - Где: `mapping/params.py:57-78`.
   - Проба: `Entity(make_loan, 'id', extensions=Append('extension'))` без `extensions` в сигнатуре → `extensions=[]` без ошибки.
   - Фабрика с `**kw` даёт автоотношение `{'kw': Assign('_empty')}`.

9. **Фабрики-функции и фабрики-классы.** [средняя]
   - Функция считается dict-ом (`mapping/mapper.py:84-90`: `issubclass(function, dict)` → TypeError → accessor `'item'`):
     сгенерированный код делает `'extensions' not in loan` → `TypeError` (проверено).
   - Класс вызывается только с плоскими колонками своего префикса (`mapper.py:265-287`), `Assign` применяется после
     конструктора: обязательное поле-VO (`Loan.period`) не передать; для `frozen` — `FrozenInstanceError`.
   - Функция с именем, совпадающим с префиксом, → `UnboundLocalError`.
   - Итог: dataclass-агрегат с вложенным объектом-значением собрать нельзя.

10. **`Assign(SomeClass)` не работает.** [средняя]
    - Где: `mapping/params.py:18-21` — `target.__class__.__name__` для класса — `'type'`. Пример README:862 нерабочий.

11. **Namespace фабрик по `__name__`.** [низкая] `mapping/mapper.py:92-96`: два класса с одним именем — корень создаётся не тем классом.

12. **Ключи `default_mapping` не приводятся к нижнему регистру.** [средняя]
    - Где: `engine.py:130` — `frozendict(...)` вместо `create_mapping`: `{'FlatLoan': ...}` → `KeyError 'flatloan'`.

13. **Опечатка в `Context.parse_columns`.** [низкая/средняя] `mapping/mapper.py:117`: `param not in self.mapper` вместо `prefix`.

14. **Коллекции и `all()`.** [средняя]
    - Строки обязаны идти отсортированными по корню: на несортированных корень выдаётся дважды (`[Loan1, Loan2, Loan1]`),
      хотя docstring `mapping/query.py:79-82` обещает обратное.
    - Две коллекции `Value` в одном JOIN дают дубли детей (дедупликация есть только для `Entity`).
    - Каждая колонка обязана иметь вид `prefix__field` (`mapper.py:105-115`): `COUNT(*) OVER()` не добавить.
    - Загрузка агрегата с коллекцией одним запросом при этом работает (JOIN + `Append` + identity map).

15. **Неизвестный драйвер падает.** [средняя] `engine.py:119`, `pool.py:84` — `KeyError`; README:196 обещает «валидация отключается».

16. **MySQLdb/pymysql/pymssql: нет autocommit.** [средняя, не проверено]
    - `enable_autocommit` = `pass` (`backends/mysqldb.py:13`, `pymysql.py:13`, `pymssql.py:13`), `before_release` делает rollback:
      запись вне `transaction()` откатывается, хотя заявлен autocommit (`engine.py:38`).

17. **Вложенная `transaction()` внутри `transaction(readonly=True)` → `AssertionError`** (`transaction.py:74`), `assert` исчезает при `-O`. [средняя]

18. **Статические шаблоны читаются без кодировки.** [средняя] `templates/static/template.py:26` — на Windows (cp1251) UTF-8 исказится.

19. **`Query._prepare` игнорирует `kwargs` при переданном dict.** [средняя]
    - Где: `query.py:358-365`. Проба: `execute({'value': 1}, value=2)` записала 1; README:477-483 обещает слияние.
    - `MapperQuery._prepare` не принимает tuple.

20. **Пул.** [средняя/низкая]
    - Вне транзакции на каждый запрос: `SELECT 1` при acquire, rollback и `SELECT 1` при release (`pool.py:100-111`, `conn_validator.py:12-27`).
    - Нет `Engine.close()`. Не используются `pool.connect()`/`ContextManagerWrappedConnection`, `logger`, `Engine._pool_cls`.

21. **Типизация (pyright strict).** [средняя]
    - Нет своего `py.typed` (`classic/py.typed` принадлежит classic-criteria) → `reportMissingTypeStubs`.
    - `fetchone() -> Row` без `None` (`dbapi.py:33`), хотя сама библиотека проверяет `row is None` (`query.py:275`) —
      в пользовательском коде `row is None` даёт `reportUnnecessaryComparison`.
    - `Query.one()`/`scalar()` возвращают `Any`.
    - `Result = TypeVar('Result', bound=Type[object])` (`mapping/types.py:7`, `mapping/query.py:16`):
      `map_to(Task).one()` выводится как `type[Task] | None` вместо `Task | None`.
    - `Factory = Union[Type[Any], Callable[[Any], Any]]` (`mapping/params.py:11`): функции с keyword-only/`**kw` не подходят.
    - `Engine(sqlite3)` — ошибка: `DBModule.paramstyle: str` инвариантен к `Literal['qmark']` (`dbapi.py:60`).
    - `MapperFunc` с несвязанным TypeVar (`mapping/types.py:10-15`).

22. **Мелочи.** [низкая]
    - `engine.py:141-145`: ValueError с `%`-аргументами без форматирования; `:199-202` — `tmpl` не определена при `static` не bool.
    - Статический кеш: ключ `filename` совпадает с ключом `content` — `query('x.sql', static=True)` пересекается с `query_from('x.sql')`.
    - `Transaction.__enter__` возвращает `None`. Дубли валидаторов (mysqldb, pymysql, sqlite3).
    - f-string `level` в SQL в бэкендах (`mysqldb.py:22` и др.) — инъекция только из кода.

23. **Покрытие тестами.** [средняя]
    - Нет тестов: `traverse`, два `inclause`, повтор переменных, вложенные транзакции psycopg, сбой commit, `Assign(Class)`,
      фабрики-функции, строковые аннотации, несортированный `all()`, многопоточность.
    - AGENTS.md говорит, что тестам нужен PG, но conftest работает на sqlite.

## Замечания к документации

**README — неверные примеры (проверено):**
- :113 `@engine.conn()` → `TypeError`; работает только `@engine.conn`. Декоратора `@engine.transaction(commit=False)` нет.
- :261 `query('SELECT {{ value }}', static=True)` уходит в БД как есть; :280 противоречит («static=False»).
- :302 `{{ value|bind }}` падает; :318 фильтр называется `sqlsafe`, а не `safe`.
- :334-336 `[...]` для MSSQL не поддерживается (`dynamic/cache.py:26`).
- :477-483 слияния dict и kwargs нет.
- :527-536 `.sources()` возвращает исходник маппера, а не SQL, и выполняет запрос.
- :553-567 `Eq`, `Gt` в classic-criteria нет, `my_translators` не определён.
- :655 разделитель `__`, а не `_`; :658 префикс `SomeObj` при маппинге `task`.
- :774 «третий аргумент — маппер (устаревший)» не существует; :813 лишний `ConnectionPool(...)`.
- :862 `Assign(Status)` и :884 «можно указывать классы» не работают.
- :961-962 `execute()` возвращает курсор, у него нет `task.id`.
- :196 неизвестный драйвер не поддерживается; :520-523 логируется только SQL на DEBUG.

**Нет совсем:**
- параметры транзакции `readonly`/`level`/`deferrable` и поддержка по драйверам; вложенность;
- `ConnectionScope`/`Scope`, `engine.current`; что `iter`/`scalars`/`execute` вызываются внутри `conn()`/`transaction()`;
- построение `Engine` в приложении, закрытие пула; `templates_dirs` (`Path`, список, поиск `import` в Jinja);
- интеграция criteria: результат `traverse` выводится через `| safe` (иначе весь WHERE биндится одним параметром),
  макрос называется по имени класса критерия, уникальные имена bind, скобки;
- маппинг вложенных VO и фабрик-функций (ограничения), коллекции, обязательный `ORDER BY` по корню,
  ограничение на две `Value`-коллекции, составные ключи (работают: `Entity(C, ('a','b'))`), пагинация корней;
- экспорт `ConnectionLimitError`, типизация `map_to`.

**AGENTS.md:** тесты не требуют PostgreSQL (sqlite); `example.sql` нет; замечание про баг DB_PORT в conftest неактуально.
**CHANGELOG:** нет 3.2/3.3; упоминается несуществующий `test_mapper_cache.py`.

## Рецепты, работающие в текущей версии

Уникальные bind в макросе критерия (проверено рендером, pyformat и qmark):
```jinja
{% set ns = namespace(i=0) %}
{% macro of_reader(criteria) -%}{% set ns.i = ns.i + 1 %}(l.reader_id = {{ criteria.reader_id | bind('of_reader_' ~ ns.i) | sqlsafe }}){%- endmacro %}
```
`| sqlsafe` после `bind(...)` обязателен (п. 6).

Агрегат с коллекцией и пагинацией по корням одним запросом:
```sql
WITH roots AS (SELECT l.id FROM loans l WHERE {{ traverse(criteria, macros) | safe }} ORDER BY l.id LIMIT {{ limit }} OFFSET {{ offset }})
SELECT ... FROM roots JOIN loans l ON l.id = roots.id LEFT JOIN loan_extensions e ON ... ORDER BY l.id, e.position
```

## План задач

| # | Задача | Объём | Статус |
|---|---|---|---|
| L1 | Уникальные имена bind (с учётом paramstyle), `inclause` с уникальными именами, пустой список, повторы для qmark/format/numeric, двойной bind после `bind('x')` | M | сделано (`0aae33e`) |
| L2 | Убрать `return` из `finally` в `Transaction.__exit__`, тест на сбой commit | S | сделано (`6a7669b`) |
| L3 | psycopg savepoint через `sql.Identifier`, тесты вложенных транзакций на PG | S | сделано (`391659e`), проверено на PostgreSQL 14 |
| L4 | `traverse`: скобки для And/Or/Not, XOR без оператора `XOR`, понятная ошибка при отсутствии макроса | S | сделано (`b956f94`, повторный рендер XOR — `0aae33e`) |
| L5 | Типизация: `fetchone() -> Row \| None`, `one()`, `map_to(type[T]) -> MapperQuery[T]`, `Factory = Callable[..., Any]`, `paramstyle`, `py.typed` | M | |
| L6 | Маппер: accessor для функций, не терять отношения, строковые аннотации, `Assign(Class)`, lowercase `default_mapping`, опечатка `prefix`, namespace, дедуп корня в `all()`, колонки без `__` | M/L | |
| L7 | Сборка VO из колонок корня (`Loan.period`) | L | |
| L8 | `iter`/`scalars`/`execute`: держать соединение до исчерпания генератора или ошибка вне scope | M | сделано (`0a0cb98`): `Rows` держит соединение, `execute()` вне scope — `DetachedCursor` |
| L9 | Неизвестный драйвер, autocommit MySQL/MSSQL, `encoding='utf-8'`, `_prepare`, `Engine.close()`, мёртвый код | M | |
| D1 | Исправить неверные примеры README | S | |
| D2 | Новые разделы: транзакции, scope, Engine, `templates_dirs`, criteria, маппинг, пагинация | M | |
| D3 | AGENTS.md и CHANGELOG | S | |
| P1 | Кодген маппера: позиционные аргументы конструкторов вместо kwargs (`render_factory_call`). Порядок аргументов — из `inspect.signature` фабрики при компиляции; fallback на kwargs при keyword-only, `**kw`, недоступной сигнатуре и dict-фабриках | M | сделано (`6b5d3c6`): all()-путь 0.1005→0.0833 s (+21%) на 100k строк Entity+Append, Python 3.14.6; цена — compile 0.41→0.51 ms |
| P2 | Кодген маппера: list-builder для `MapperQuery.all()` вместо генератора | S/M | проверено, **отклонено**: на CPython 3.14 `list()` по генератору быстрее list-builder'а — 0.078 vs 0.083 s (с hoisted `result.append`; без — 0.091). Выигрыш hand-inlined варианта (1.27x) был от инлайна `map_*`/кортежей, не от устранения генератора. Оставлен генератор для всех путей |
| P3 | Гипотеза: переписать кодген маппера с raw AST на текстовую генерацию + `compile()`. Ожидание: `mapping/mapper.py` меньше на ~500 строк, проще аудит и сопровождение, `.sources()` бесплатно (не нужен `ast.unparse`), вероятно быстрее компиляция (сейчас 0.51 ms на маппер через AST, после P1). Риски: санитизация идентификаторов (AST допускает алиасы вида `my-task__id`, текст — нет), коллизии фабрик по `__name__` (п. 11), экранирование. Проверка: прототип на форме Entity+Append, сравнение compile time и runtime, полный прогон тестов | M | гипотеза, к проверке |

Вне плана: classic-criteria поднят до `~=1.0` (`84d3d9a`); `uv.lock` обновить после выхода classic-criteria 1.0 на PyPI, до пуша.
