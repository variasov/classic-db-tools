# classic-db-tools

Python library for SQL queries as Jinja2 templates. Targets multiple DB drivers (psycopg, psycopg2, pymysql, mysqldb, pymssql, oracledb, cx_oracle).

## Commands

- `uv sync` — install all deps (including dev)
- `uv run ruff check sources/` — lint
- `uv run pytest tests/` — run all tests (requires running PostgreSQL)
- `uv build` — build package

## Project layout

```
sources/classic/db_tools/   ← namespace package: classic.db_tools
tests/                       ← pytest, requires PostgreSQL (see conftest.py)
example.sql                  ← sample SQL
```

Package source: `sources/classic/db_tools/`. Tests live at repo root `tests/`.

## Testing

Tests require a local PostgreSQL instance. Connection configured via env vars:
- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432` — note: conftest.py has a bug where it reads `DB_HOST` for port)
- `DB_NAME` (default: `tasks`)
- `DB_USER` / `DB_PASSWORD` (default: `test`/`test`)

There is no way to run tests without a database. Many tests use a session-scoped `engine` fixture that creates a connection on first use.

## Code style

- 4-space indent, LF line endings, 80 char max line length (`.editorconfig`)
- Ruff with defaults (no `[tool.ruff]` section in pyproject.toml)
- Python 3.10+
- README and most docs are in Russian
