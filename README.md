# SQLBuster

SQLBuster is a Python tool for exploring and testing the boundaries of database SQL capabilities. It uses a Depth-First Search (DFS) algorithm to progressively evolve SQL statements and explore how databases support various SQL clauses.

## 🌟 Features

- **Automated SQL Exploration**: Automatically generates and tests various SQL statement variants
- **Modular Design**: Supports flexible clause generators and runner extensions
- **Multiple Report Formats**: Supports JSON and text tree report outputs
- **Statistical Analysis**: Provides detailed execution statistics and success rate analysis
- **Easy to Extend**: Clear architectural design for adding new features
- **Representative Sampling Optimization**: Intelligently reduces redundant tests and improves exploration efficiency
- **CTE, JOIN, WINDOW, UNION Support**: Supports CTE (WITH), JOIN, WINDOW functions, and UNION clauses
- **SQL Function Support**: Supports SQL-92 standard functions and custom user-defined functions in generated SQL statements
- **Preview SQL Generation**: Preview generated SQL statements without executing them, useful for seeing what SQLs would be produced

## 🚀 Representative Sampling Optimization

### Background

When exploring SQL capability boundaries, the current system may generate clause variants for every column in a table. For example, if a table has 3 columns of type int, the WHERE clause generator will create 3 variants:
- WHERE int_col1 > 1
- WHERE int_col2 > 1  
- WHERE int_col3 > 1

This leads to a large number of redundant tests.

### Optimization Strategy

**Representative Sampling** optimizes testing efficiency through the following mechanism:

1. **Equivalence Class Partitioning**: Columns of the same type belong to the same equivalence class
2. **Representative Testing**: If a column of a certain type succeeds in testing for a clause, other columns of that type are no longer enumerated
3. **Direct Skip**: After successful testing, directly jump to the next level clause

### Technical Implementation

#### 1. Type Success Cache

Added type success cache in `EvolvingEngine`:
```python
# Type success cache: records which clauses have succeeded for each type
# Structure: { (table_name, clause_type, column_type): True }
self._type_success_cache: Dict[Tuple[str, ClauseType, type], bool] = {}
```

#### 2. Clause Generator Interface Update

All clause generators now support an optional `type_success_cache` parameter:
```python
def generate(
    self, 
    clause_type: ClauseType,
    registry: Optional[SchemaRegistry] = None,
    type_success_cache: Optional[Dict] = None
) -> List[ClauseVariant]:
    """
    Generate clause variants
    
    Args:
        clause_type: Clause type
        registry: Schema registry (optional)
        type_success_cache: Type success cache for representative sampling optimization
    """
```

#### 3. Representative Sampling Logic

Implemented in each clause generator:
- Check if the type has already succeeded in `type_success_cache`
- If successful, generate only one representative variant (using the first column of that type)
- If not successful, generate variants for all columns (existing logic)

Example (WHERE clause generator):
```python
# For each type, check if there's already a success record
for col_type, cols in columns_by_type.items():
    cache_key = (table_name, ClauseType.WHERE, col_type)
    if type_success_cache and cache_key in type_success_cache:
        # Already succeeded, generate only one representative variant
        col = cols[0]
        variant_sql = self._generate_condition(col)
        variants.append(ClauseVariant(
            clause_type=ClauseType.WHERE,
            sql_fragment=variant_sql,
            priority=1,
            metadata={"column": col.name, "type": col_type.__name__, "representative": True}
        ))
    else:
        # Not succeeded, generate all variants
        for col in cols:
            variant_sql = self._generate_condition(col)
            variants.append(ClauseVariant(...))
```

### Benefits

- **Reduced Redundant Tests**: For types that have already succeeded, no longer repeat tests for other columns of the same type
- **Improved Exploration Efficiency**: Significantly reduces unnecessary SQL execution counts
- **Backward Compatible**: If `type_success_cache` is not passed, behavior remains consistent with the original

### Example Output

After enabling representative sampling, logs will show:
```
INFO - WHERE clause using representative sampling: type=int, column=age
INFO - GROUP BY clause using representative sampling: type=str, column=name
```

## 🔍 Preview SQL Generation

The `preview()` method on `EvolvingEngine` lets users preview generated SQL statements without executing them. This is useful to see what SQLs would be produced after registering schemas, without needing an actual database connection.

### Key Benefits

- **No Database Required**: Set `runner=None` when creating the engine for preview mode
- **Quick Feedback**: See generated SQL patterns before running expensive exploration
- **Debugging Aid**: Understand what SQL variations the system will produce
- **Type Success Cache Support**: Optionally pass a pre-populated cache to simulate representative sampling behavior

### Usage

```python
from sqlbuster.core.engine import EvolvingEngine, ClauseType
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry
from sqlbuster.clauses.base import register_clause_generator
from sqlbuster.clauses.select import SelectClauseGenerator

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

### Parameters

- `max_sql_count`: Maximum number of SQLs to generate (default: 100, None = unlimited)
- `type_success_cache`: Optional pre-populated cache to simulate representative sampling (default: None/empty)

### Returns

A list of SQL strings that would be generated during `explore()`.

### Example Output

```
SELECT id FROM users
SELECT name FROM users
SELECT id, name FROM users
SELECT * FROM users
...
```

## 🧮 SQL Function Support

SQLBuster now supports SQL functions in generated statements. This feature enables the generation of SQL queries that use functions in SELECT and WHERE clauses.

### Features

- **SQL-92 Standard Functions**: Built-in support for SQL-92 standard functions
- **Custom Function Registration**: Users can register their own custom functions
- **Automatic Function Application**: Clause generators can automatically apply functions to columns
- **Function Categories**: Functions are organized by categories (aggregate, string, numeric, datetime, etc.)

### Supported SQL-92 Functions

#### Aggregate Functions
- `COUNT(*)` / `COUNT(expr)` / `COUNT(DISTINCT expr)`
- `SUM(expr)` / `SUM(DISTINCT expr)`
- `AVG(expr)` / `AVG(DISTINCT expr)`
- `MAX(expr)`
- `MIN(expr)`

#### String Functions
- `UPPER(string)` - Convert to uppercase
- `LOWER(string)` - Convert to lowercase
- `TRIM(string)` - Remove leading/trailing spaces
- `SUBSTRING(string FROM start [FOR length])` - Extract substring
- `CHAR_LENGTH(string)` - Get string length
- `CONCAT(string1, string2, ...)` - Concatenate strings

#### Numeric Functions
- `ABS(numeric)` - Absolute value
- `MOD(n, m)` - Remainder of division
- `ROUND(numeric [, decimals])` - Round to specified decimals
- `FLOOR(numeric)` - Largest integer not greater than value
- `CEILING(numeric)` - Smallest integer not less than value
- `POWER(numeric, exponent)` - Raise to power
- `SQRT(numeric)` - Square root

#### Datetime Functions
- `CURRENT_DATE` - Current date
- `CURRENT_TIME` - Current time
- `CURRENT_TIMESTAMP` - Current timestamp
- `EXTRACT(field FROM datetime)` - Extract field from datetime

#### Conversion Functions
- `CAST(expr AS type)` - Cast to specified type

#### Conditional Functions
- `COALESCE(value1, value2, ...)` - Return first non-null value
- `NULLIF(value1, value2)` - Return NULL if values equal

### Using Function Registry

The `FunctionRegistry` manages all SQL functions. It's automatically used by the SELECT and WHERE clause generators.

```python
from sqlbuster.core.function_registry import FunctionRegistry

# Create registry (automatically loads SQL-92 functions)
registry = FunctionRegistry()

# Get a function
func = registry.get_function("UPPER")
print(f"Function: {func.name}, Returns: {func.return_type}")

# Generate SQL for a function
sql = registry.generate_sql("UPPER", {"string": "name"})
print(sql)  # Output: UPPER(name)

# Generate SQL with DISTINCT
sql = registry.generate_sql("COUNT", {"expr": "age"}, use_distinct=True)
print(sql)  # Output: COUNT(DISTINCT age)

# List all functions
print(registry.list_functions())
```

### Registering Custom Functions

You can register your own custom functions to be used in SQL generation:

```python
from sqlbuster.core.function_registry import FunctionRegistry

registry = FunctionRegistry()

# Register a custom function
registry.register_custom_function(
    name="DOUBLE_VALUE",
    return_type="numeric",
    parameters=[
        {"name": "value", "type": "numeric"},
    ],
    sql_template="({value} * 2)",
    description="Double the input value"
)

# Use the custom function
sql = registry.generate_sql("DOUBLE_VALUE", {"value": "price"})
print(sql)  # Output: (price * 2)
```

### Custom Function Parameters

When registering custom functions, you can specify:

- **name**: Function name
- **return_type**: Return type (e.g., "string", "numeric", "integer")
- **parameters**: List of parameter definitions, each with:
  - `name`: Parameter name
  - `type`: Parameter type
  - `optional` (optional): Whether the parameter is optional
  - `variadic` (optional): Whether the parameter accepts variable number of arguments
  - `default` (optional): Default value for optional parameters
- **sql_template**: SQL template with `{param_name}` placeholders
- **description**: Optional description

Example with optional and variadic parameters:

```python
# Function with optional parameter
registry.register_custom_function(
    name="GREET",
    return_type="string",
    parameters=[
        {"name": "name", "type": "string"},
        {"name": "greeting", "type": "string", "optional": True, "default": "'Hello'"},
    ],
    sql_template="CONCAT({greeting}, ' ', {name})",
)

# Function with variadic parameter
registry.register_custom_function(
    name="SUM_MULTIPLE",
    return_type="numeric",
    parameters=[
        {"name": "values", "type": "numeric", "variadic": True},
    ],
    sql_template="({values})",  # Will join with +
)
```

### Integration with Clause Generators

The SELECT and WHERE clause generators automatically use the FunctionRegistry to generate SQL with functions:

```python
from sqlbuster.core.engine import EvolvingEngine, ClauseType
from sqlbuster.core.registry import SchemaRegistry, ColumnSchema
from sqlbuster.core.runner import MockRunner
from sqlbuster.clauses.base import register_clause_generator
from sqlbuster.clauses.select import SelectClauseGenerator
from sqlbuster.clauses.where import WhereClauseGenerator

# Setup
registry = SchemaRegistry()
columns = [
    ColumnSchema(name="id", type=int, is_nullable=False),
    ColumnSchema(name="name", type=str),
    ColumnSchema(name="age", type=int),
]
registry.register_table("users", columns)

runner = MockRunner()
engine = EvolvingEngine(registry=registry, runner=runner, start_table="users")

# Register generators (they automatically use FunctionRegistry)
register_clause_generator(engine, ClauseType.SELECT, SelectClauseGenerator, registry)
register_clause_generator(engine, ClauseType.WHERE, WhereClauseGenerator, registry)

# Explore - generated SQL may include functions like:
# SELECT UPPER(name) FROM users
# WHERE ABS(age) > 1
for node in engine.explore():
    print(f"SQL: {node.sql}")
```

## 📦 Installation

### Install from Source

```bash
git clone <repository-url>
cd sqlbuster
pip install -e .
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

Minimum dependencies:
- Python 3.8+
- pytest (for testing)

## 🚀 Quick Start

### Basic Usage Example

```python
from sqlbuster.core.engine import EvolvingEngine, ClauseType
from sqlbuster.core.registry import SchemaRegistry, TableSchema, ColumnSchema
from sqlbuster.core.runner import MockRunner
from sqlbuster.clauses.base import register_clause_generator

# 1. Create schema registry
registry = SchemaRegistry()
columns = [
    ColumnSchema(name="id", type=int, is_nullable=False),
    ColumnSchema(name="name", type=str),
    ColumnSchema(name="age", type=int),
]
registry.register_table("users", columns)

# 2. Create SQL runner (using mock runner here)
runner = MockRunner()

# 3. Create evolution engine
engine = EvolvingEngine(
    registry=registry,
    runner=runner,
    start_table="users"
)

# 4. Register clause generators (using the new register_clause_generator function)
from sqlbuster.clauses.select import SelectClauseGenerator
from sqlbuster.clauses.where import WhereClauseGenerator

register_clause_generator(engine, ClauseType.SELECT, SelectClauseGenerator, registry)
register_clause_generator(engine, ClauseType.WHERE, WhereClauseGenerator, registry)
# ... register other generators

# 5. Start exploration
for node in engine.explore():
    print(f"SQL: {node.sql}, Success: {node.success}")

# 6. View type success cache (representative sampling status)
type_cache = engine.get_type_success_cache()
print(f"Cached type-clause combinations: {len(type_cache)}")
```

### Generate Reports

```python
from sqlbuster.reporter import Reporter

reporter = Reporter()

# Add execution results
for node in engine.explore():
    reporter.add_execution_result(node)

# Generate JSON report
json_report = reporter.generate_json_report()
print(json_report)

# Generate summary
summary = reporter.generate_summary()
print(summary)

# Save to file
reporter.save_report("report.json", format="json")
```

## 📚 Module Documentation

### Core Modules (`sqlbuster/core/`)

- **engine.py**: Evolution engine implementing DFS exploration algorithm with representative sampling optimization
- **registry.py**: Schema registry managing database table structure information
- **runner.py**: SQL runner abstract interface and mock implementation

### Clause Generators (`sqlbuster/clauses/`)

- **base.py**: Clause generator base class supporting representative sampling interface
- **cte.py**: CTE (WITH) clause generator (supports representative sampling)
- **select.py**: SELECT clause generator
- **join.py**: JOIN clause generator (supports representative sampling)
- **where.py**: WHERE clause generator (supports representative sampling)
- **group_by.py**: GROUP BY clause generator (supports representative sampling)
- **having.py**: HAVING clause generator (supports representative sampling)
- **window.py**: WINDOW clause generator (supports representative sampling)
- **order_by.py**: ORDER BY clause generator (supports representative sampling)
- **limit.py**: LIMIT clause generator
- **union.py**: UNION clause generator (supports representative sampling)

### Reporter Module (`sqlbuster/reporter/`)

- **reporter.py**: Report generator supporting multiple output formats

### Utility Modules (`sqlbuster/utils/`)

- **errors.py**: Custom exception classes
- **types.py**: Type definitions

## 🧪 Running Tests

```bash
# Run all tests
python -m pytest sqlbuster/tests/ -v

# Run tests with coverage report
python -m pytest sqlbuster/tests/ --cov=sqlbuster --cov-report=html

# Run specific test file
python -m pytest sqlbuster/tests/test_engine.py -v
```

### Testing Representative Sampling

Test files include specific test cases for representative sampling:
- `TestRepresentativeSampling`: Tests type success cache and sampling logic
- `TestWhereClauseGeneratorRepresentativeSampling`: Tests WHERE generator sampling behavior

## 📁 Project Structure

```
sqlbuster/
├── sqlbuster/
│   ├── __init__.py
│   ├── clauses/          # Clause generators
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── cte.py        # CTE (WITH) clause generator
│   │   ├── select.py
│   │   ├── join.py       # JOIN clause generator
│   │   ├── where.py
│   │   ├── group_by.py
│   │   ├── having.py
│   │   ├── window.py     # WINDOW clause generator
│   │   ├── order_by.py
│   │   ├── limit.py
│   │   └── union.py      # UNION clause generator
│   ├── core/             # Core modules
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   ├── registry.py
│   │   └── runner.py
│   ├── examples/         # Example code
│   │   ├── __init__.py
│   │   └── mock_runner_demo.py
│   ├── reporter/         # Report generation
│   │   ├── __init__.py
│   │   └── reporter.py
│   ├── tests/            # Unit tests
│   │   ├── __init__.py
│   │   ├── test_engine.py
│   │   ├── test_registry.py
│   │   ├── test_runner.py
│   │   └── test_reporter.py
│   └── utils/            # Utility modules
│       ├── __init__.py
│       ├── errors.py
│       └── types.py
├── plans/                # Design documents
│   └── architecture.md
├── requirements.txt      # Project dependencies
├── pyproject.toml       # Project configuration
├── README.md            # Project documentation (English)
├── README_CN.md         # Project documentation (Chinese)
└── LICENSE              # License
```

## 🔧 Extending Development

### Adding a New Clause Generator

1. Create a new generator file in `sqlbuster/clauses/` directory
2. Inherit from `BaseClauseGenerator` class
3. Implement the `generate` method (support optional `type_success_cache` parameter for representative sampling)
4. Register the new generator using the `register_clause_generator` function

Example:
```python
from sqlbuster.clauses.base import BaseClauseGenerator, register_clause_generator
from sqlbuster.core.engine import ClauseType, ClauseVariant

class MyClauseGenerator(BaseClauseGenerator):
    def generate(
        self, 
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None
    ) -> List[ClauseVariant]:
        # Implement your generation logic
        # Can utilize type_success_cache for representative sampling
        pass

# Register generator
register_clause_generator(engine, ClauseType.MY_CLAUSE, MyClauseGenerator, registry)
```

### Implementing a Custom SQL Runner

```python
from sqlbuster.core.runner import BaseSQLRunner

class MyCustomRunner(BaseSQLRunner):
    def execute_sql(self, sql: str) -> tuple[bool, str | None]:
        # Implement your SQL execution logic
        # Return (success or not, error message)
        pass
    
    def get_supported_clauses(self) -> set[str]:
        # Return set of supported clause types
        pass
```

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Issues and Pull Requests are welcome!

## 📧 Contact

For questions or suggestions, please contact via:
- Submit a GitHub Issue
- Send email to: [iamjrluo at outlook.com]

---

**Note**: This project is currently in development, and APIs may change.

## 🎯 Performance Comparison

After enabling representative sampling optimization, the number of tests is significantly reduced for tables with multiple columns of the same type:

| Scenario | Tests Before Optimization | Tests After Optimization | Reduction |
|----------|---------------------------|------------------------|-----------|
| 3 int columns, WHERE test | 3 | 1 | 66.7% |
| 5 str columns, ORDER BY test | 10+ | 2 | 80%+ |

Actual results depend on table structure and column type distribution.
