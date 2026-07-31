# Changelog

## v3.1.0 (2026-07-31)

### Добавлено

- **Вложенные транзакции (Savepoint)**: реализована поддержка вложенных транзакций через SQL SAVEPOINT
- **Поддержка SQLite**: добавлен backend `Sqlite3Transaction` и `Sqlite3ConnectionValidator` для работы с SQLite через модуль `sqlite3` (включая savepoint)

---

## v3.0.0

Крупный рефакторинг кодовой базы.

### Изменения

- **Новая структура пакета**: шаблоны вынесены в `templates/dynamic/` и `templates/static/`, добавлен `templates/template.py` с базовым классом
- **Backends**: добавлен модуль `backends/` с драйвер-специфичными реализациями транзакций (`PsycopgTransaction`, `Psycopg2Transaction`, `PyMySQLTransaction`, `MySQLDBTransaction`, `PyMSSQLTransaction`, `OracleDBTransaction`, `CxOracleTransaction`) и валидаторов подключений
- **Транзакции переписаны**: теперь `Transaction` использует `__init_subclass__` для регистрации реализаций под конкретный драйвер; поддержка savepoint и параметров транзакции (`readonly`, `level`, `deferrable`)
- **ConnectionPool переписан**: убран `poolvalidators.py`, валидация перенесена в `conn_validator.py`; пул использует единый `queue.Queue`
- **Scope**: добавлен `scope.py` с `threading.local` для хранения текущего соединения и параметров транзакции
- **ConnectionScope**: новый контекстный менеджер для захвата/освобождения соединения
- **Mapper переименован из compiler.py** и доработан: AST-компиляция маппера с кешированием
- **MapperQuery**: выделен в отдельный `mapping/query.py` с типизированными методами `all()`, `one()`, `iter()`, `sources()`
- **DynamicTemplatesCache / StaticTemplatesCache**: новый механизм кеширования шаблонов через `RLock`
- **Добавлены докстринги** ко всем публичным классам и методам
- **Удалены** устаревшие модули: `doublewrap.py`, `dynamic/factory.py`, `static/factory.py`, `params_styles.py`, `poolvalidators.py`, `scoped_connection.py`, `exceptions.py`
- **Тесты**: переписаны тесты транзакций, маппинга (obj, dict, typed-dict), примеров; добавлены тесты `test_mapper_cache.py`
- **README**: полностью обновлён (добавлены методы `.scalars()`, `scalar(raising=True)`, `engine.conn()` как декоратор, `map_to(**params)`)
- **AGENTS.md**: добавлен файл конфигурации для opencode
- Зависимости: обновлён `pyproject.toml` (Python 3.10+, frozendict)

---

## v2.0.3 (2026-04-30)

### Исправлено

- **Value**: исправлен баг возвратом `Value` в результатах маппера ([#18](https://github.com/variasov/classic-db-tools/pull/18))

---

## v2.0.2 (2026-03-31)

### Исправлено

- **Criteria**: исправлен баг с импортом `Criteria` при отсутствии установленного пакета `classic-criteria` ([#17](https://github.com/variasov/classic-db-tools/pull/17))

---

## v2.0.1 (2026-03-30)

### Исправлено

- **Маппинг**: исправлен баг с повторением сущностей в списке при прикреплении сущности к чему-либо ([#16](https://github.com/variasov/classic-db-tools/pull/16))
- **Маппинг**: исправлен баг с отсутствием кастомных `Assign` при использовании одного и того же класса
- **Параметры маппинга**: изменена подача параметров; путь к директории с запросами теперь опциональный ([#15](https://github.com/variasov/classic-db-tools/pull/15))

---

## v2.0.0 (2026-03-23)

### Добавлено

- **classic-criteria**: интеграция с `classic-criteria` через Jinja макросы `traverse`/`contains`; поддержка переименованного `classic-domain` ([#14](https://github.com/variasov/classic-db-tools/pull/14))

### Исправлено

- **Add**: исправлено поведение `Add` для множеств ([#13](https://github.com/variasov/classic-db-tools/pull/13))

---

## v1.0.0

### Добавлено

Первая версия.
