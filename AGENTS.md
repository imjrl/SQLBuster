# AGENTS.md

## Commands

```bash
# Run all tests (pytest config in pyproject.toml, testpaths = ["sqlbuster/tests"])
pytest

# Run a single test file
pytest sqlbuster/tests/test_engine.py

# Run a specific test class or method
pytest sqlbuster/tests/test_engine.py::TestClauseType
pytest sqlbuster/tests/test_engine.py::TestClauseType::test_clause_type_values

# Format code (black + isort, config in pyproject.toml)
black sqlbuster/
isort sqlbuster/

# Type check
mypy sqlbuster/
```

## Architecture

- **Entry point**: `EvolvingEngine` in `sqlbuster/core/engine.py`
- **Clause order is fixed**: CTE → NESTED_SUBQUERY → SELECT → JOIN → WHERE → GROUP BY → HAVING → WINDOW → ORDER BY → LIMIT → UNION (see `ClauseType.next` property)
- **Pattern**: Registry (`SchemaRegistry`) holds table schemas; Runner (`BaseSQLRunner`) executes SQL; Engine orchestrates DFS exploration
- **Clause generators**: Each in `sqlbuster/clauses/` inherits `BaseClauseGenerator`, registered via `register_clause_generator()`
- **New clause types**: 
  - `CTE` (WITH): Common Table Expression, wraps entire query
  - `NESTED_SUBQUERY`: Nested subquery support (derived tables, WHERE subqueries, scalar subqueries)
  - `JOIN`: Joins multiple tables
  - `WINDOW`: Window functions for analytics
  - `UNION`: Combines multiple SELECT statements

## Key Quirks

- **Representative sampling**: When a column type succeeds for a clause, other columns of same type are skipped. Cache in `EvolvingEngine._type_success_cache`
- **Clause generators take `type_success_cache` param** (check with `inspect.signature` for backward compat in `engine.py:491`)
- **SQL building is context-aware**: HAVING requires GROUP BY present; `_build_sql()` in engine handles insertion order
- **`__init__.py` files are empty** — package exports are not centralized

## Config Notes

- `pyproject.toml` has pytest `addopts = "-v --tb=short"` (already verbose)
- black line-length = 88, isort profile = "black"
- mypy: `ignore_missing_imports = true`, `disallow_untyped_defs = true`

## Features

### Preview SQL Generation

The `preview()` method on `EvolvingEngine` lets users preview generated SQL statements without executing them. Useful to see what SQLs would be produced after registering schemas.

**Usage**:
```python
from sqlbuster.core.engine import EvolvingEngine, ClauseType
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry
from sqlbuster.clauses.base import register_clause_generator
# ... import clause generators

# 1. Register schemas
registry = SchemaRegistry()
columns = [ColumnSchema(name="id", type=int), ColumnSchema(name="name", type=str)]
registry.register_table("users", columns)

# 2. Create engine (runner can be None for preview)
engine = EvolvingEngine(registry=registry, runner=None, start_table="users")

# 3. Register clause generators
register_clause_generator(engine, ClauseType.SELECT, SelectClauseGenerator, registry)
# ... register other generators

# 4. Preview generated SQLs
sqls = engine.preview(max_sql_count=10)
for sql in sqls:
    print(sql)
```

**Parameters**:
- `type_success_cache`: Optional pre-populated cache to simulate representative sampling (default: None/empty)
- `max_sql_count`: Max number of SQLs to generate (default: 100, None = unlimited)

**Returns**: List of SQL strings that would be generated during `explore()`.
