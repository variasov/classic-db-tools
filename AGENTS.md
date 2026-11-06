# classic-db-tools

Python library for SQL queries as Jinja2 templates. Targets multiple DB drivers (psycopg, psycopg2, pymysql, mysqldb, pymssql, oracledb, cx_oracle, sqlite3).

## Commands

- `uv sync` — install all deps (including dev)
- `uv run pytest` — run tests (sqlite in-memory, no external DB needed)
- `uv run ruff check src tests` — lint (ruff `ALL` with explicit ignores)
- `uv run ruff format --check src tests` — formatting check
- `uv run pyright` — type check (`strict`)
- `uv run vulture` — dead code check
- `uv build` — build package

## Project layout

```
src/classic/db_tools/    ← namespace package: classic.db_tools
tests/                   ← pytest (sqlite in-memory; see conftest.py)
tests/backends/          ← per-driver tests (pytest.importorskip)
tests/sql/               ← sample SQL templates used by tests
```

Package source: `src/classic/db_tools/`. Tests live at repo root `tests/`.

## Testing

The main test suite runs on sqlite3 in-memory — no external services
required. A session-scoped `engine` fixture lives in `tests/conftest.py`;
every test is wrapped in `transaction(commit=False)` and rolled back.

Driver tests in `tests/backends/` use `pytest.importorskip`: without the
driver installed they are skipped. psycopg/psycopg2 tests additionally
require a running PostgreSQL; connection is configured via env vars:
`PGHOST` (default `localhost`), `PGPORT` (`5432`), `PGDBNAME` (`test`),
`PGUSER` / `PGPASSWORD` (`test`/`test`).

## Code style

- 4-space indent, LF line endings, 80 char max line length (`.editorconfig`)
- Ruff `ALL` with explicit ignores in `[tool.ruff]` (pyproject.toml);
  both `src` and `tests` are checked
- Pyright `strict` for `src`; `tests` use a relaxed execution environment
  (`[[tool.pyright.executionEnvironments]]` in pyproject.toml)
- Python 3.10+
- README, docstrings and most docs are in Russian
