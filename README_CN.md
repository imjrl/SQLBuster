# SQLBuster

SQLBuster 是一个用于探索和测试数据库 SQL 能力边界的 Python 工具。它通过深度优先搜索（DFS）算法，逐步进化 SQL 语句，探索数据库对各种 SQL 子句的支持情况。

## 🌟 功能特性

- **自动化 SQL 探索**：自动生成和测试各种 SQL 语句变体
- **模块化设计**：支持灵活的子句生成器和执行器扩展
- **多种报告格式**：支持 JSON 和文本树形报告输出
- **统计信息分析**：提供详细的执行统计和成功率分析
- **易于扩展**：清晰的架构设计，方便添加新功能
- **代表性采样优化**：智能减少冗余测试，提高探索效率
- **预览SQL生成**：在不执行的情况下预览生成的SQL语句，方便查看将生成的SQL模式

## 🔍 预览SQL生成

`EvolvingEngine` 上的 `preview()` 方法让用户可以在不执行SQL的情况下预览生成的SQL语句。这对于在注册模式后查看将生成哪些SQL非常有用，且无需实际的数据库连接。

### 主要优点

- **无需数据库**：创建引擎时设置 `runner=None` 即可进入预览模式
- **快速反馈**：在运行耗时的探索之前查看生成的SQL模式
- **调试辅助**：了解系统将生成哪些SQL变体
- **类型成功缓存支持**：可选择传入预填充的缓存来模拟代表性采样行为

### 使用方法

```python
from sqlbuster.core.engine import EvolvingEngine, ClauseType
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry
from sqlbuster.clauses.base import register_clause_generator
from sqlbuster.clauses.select import SelectClauseGenerator

# 1. 注册模式
registry = SchemaRegistry()
columns = [ColumnSchema(name="id", type=int), ColumnSchema(name="name", type=str)]
registry.register_table("users", columns)

# 2. 创建引擎（预览时可以设置runner为None）
engine = EvolvingEngine(registry=registry, runner=None, start_table="users")

# 3. 注册子句生成器
register_clause_generator(engine, ClauseType.SELECT, SelectClauseGenerator, registry)
# ... 注册其他生成器

# 4. 预览生成的SQL
sqls = engine.preview(max_sql_count=10)
for sql in sqls:
    print(sql)
```

### 参数说明

- `max_sql_count`：最大生成SQL数量（默认：100，None表示无限制）
- `type_success_cache`：可选的预填充缓存，用于模拟代表性采样（默认：None/空）

### 返回值

返回在 `explore()` 期间将生成的SQL字符串列表。

### 示例输出

```
SELECT id FROM users
SELECT name FROM users
SELECT id, name FROM users
SELECT * FROM users
...
```

## 🚀 代表性采样优化

### 问题背景
当前系统在探测SQL能力边界时，可能会为表中的每一列都生成子句变体。例如，如果一个表有3个int类型的列，WHERE子句生成器会生成3个变体：
- WHERE int_col1 > 1
- WHERE int_col2 > 1  
- WHERE int_col3 > 1

这导致大量冗余测试。

### 优化策略
**代表性采样**（Representative Sampling）通过以下机制优化测试效率：

1. **等价类划分**：相同类型的列属于同一个等价类
2. **代表性测试**：如果某个类型的列在某个子句上测试成功，就不再枚举该类型下的其他列
3. **直接跳转**：测试成功后直接跳到下一层级子句

### 技术实现

#### 1. 类型成功缓存
在 `EvolvingEngine` 中添加了类型成功缓存：
```python
# 类型成功缓存：记录每种类型在哪些子句上已经成功
# 结构: { (table_name, clause_type, column_type): True }
self._type_success_cache: Dict[Tuple[str, ClauseType, type], bool] = {}
```

#### 2. 子句生成器接口更新
所有子句生成器现在支持可选的 `type_success_cache` 参数：
```python
def generate(
    self, 
    clause_type: ClauseType,
    registry: Optional[SchemaRegistry] = None,
    type_success_cache: Optional[Dict] = None
) -> List[ClauseVariant]:
    """
    生成子句变体
    
    Args:
        clause_type: 子句类型
        registry: 模式注册表（可选）
        type_success_cache: 类型成功缓存，用于代表性采样优化
    """
```

#### 3. 代表性采样逻辑
在各子句生成器中实现：
- 检查 `type_success_cache` 中该类型是否已成功
- 如果已成功，只生成一个代表性变体（使用第一个该类型的列）
- 如果未成功，生成所有列的变体（现有逻辑）

示例（WHERE子句生成器）：
```python
# 对每种类型，检查是否已有成功记录
for col_type, cols in columns_by_type.items():
    cache_key = (table_name, ClauseType.WHERE, col_type)
    if type_success_cache and cache_key in type_success_cache:
        # 已成功，只生成一个代表性变体
        col = cols[0]
        variant_sql = self._generate_condition(col)
        variants.append(ClauseVariant(
            clause_type=ClauseType.WHERE,
            sql_fragment=variant_sql,
            priority=1,
            metadata={"column": col.name, "type": col_type.__name__, "representative": True}
        ))
    else:
        # 未成功，生成所有变体
        for col in cols:
            variant_sql = self._generate_condition(col)
            variants.append(ClauseVariant(...))
```

### 效果
- **减少冗余测试**：对于已成功的类型，不再重复测试同类型的其他列
- **提高探索效率**：显著减少不必要的SQL执行次数
- **保持向后兼容**：如果没有传递 `type_success_cache`，行为与原先一致

### 示例输出
启用代表性采样后，日志会显示：
```
INFO - WHERE子句使用代表性采样: 类型=int, 列=age
INFO - GROUP BY子句使用代表性采样: 类型=str, 列=name
```

## 📦 安装方法

### 从源码安装

```bash
git clone <repository-url>
cd sqlbuster
pip install -e .
```

### 依赖安装

```bash
pip install -r requirements.txt
```

最低依赖：
- Python 3.8+
- pytest (用于测试)

## 🚀 快速开始

### 基本使用示例

```python
from sqlbuster.core.engine import EvolvingEngine, ClauseType
from sqlbuster.core.registry import SchemaRegistry, TableSchema, ColumnSchema
from sqlbuster.core.runner import MockRunner
from sqlbuster.clauses.base import register_clause_generator

# 1. 创建模式注册表
registry = SchemaRegistry()
columns = [
    ColumnSchema(name="id", type=int, is_nullable=False),
    ColumnSchema(name="name", type=str),
    ColumnSchema(name="age", type=int),
]
registry.register_table("users", columns)

# 2. 创建 SQL 执行器（这里使用模拟执行器）
runner = MockRunner()

# 3. 创建进化引擎
engine = EvolvingEngine(
    registry=registry,
    runner=runner,
    start_table="users"
)

# 4. 注册子句生成器（使用新的register_clause_generator函数）
from sqlbuster.clauses.select import SelectClauseGenerator
from sqlbuster.clauses.where import WhereClauseGenerator

register_clause_generator(engine, ClauseType.SELECT, SelectClauseGenerator, registry)
register_clause_generator(engine, ClauseType.WHERE, WhereClauseGenerator, registry)
# ... 注册其他生成器

# 5. 开始探索
for node in engine.explore():
    print(f"SQL: {node.sql}, Success: {node.success}")

# 6. 查看类型成功缓存（代表性采样状态）
type_cache = engine.get_type_success_cache()
print(f"已缓存的类型-子句组合: {len(type_cache)}")
```

### 生成报告

```python
from sqlbuster.reporter import Reporter

reporter = Reporter()

# 添加执行结果
for node in engine.explore():
    reporter.add_execution_result(node)

# 生成 JSON 报告
json_report = reporter.generate_json_report()
print(json_report)

# 生成摘要
summary = reporter.generate_summary()
print(summary)

# 保存到文件
reporter.save_report("report.json", format="json")
```

## 📚 模块说明

### 核心模块 (`sqlbuster/core/`)

- **engine.py**：进化引擎，实现 DFS 探索算法，包含代表性采样优化
- **registry.py**：模式注册表，管理数据库表结构信息
- **runner.py**：SQL 执行器抽象接口和模拟实现

### 子句生成器 (`sqlbuster/clauses/`)

- **base.py**：子句生成器基类，支持代表性采样接口
- **select.py**：SELECT 子句生成器
- **where.py**：WHERE 子句生成器（支持代表性采样）
- **group_by.py**：GROUP BY 子句生成器（支持代表性采样）
- **having.py**：HAVING 子句生成器（支持代表性采样）
- **order_by.py**：ORDER BY 子句生成器（支持代表性采样）
- **limit.py**：LIMIT 子句生成器

### 报告模块 (`sqlbuster/reporter/`)

- **reporter.py**：报告生成器，支持多种输出格式

### 工具模块 (`sqlbuster/utils/`)

- **errors.py**：自定义异常类
- **types.py**：类型定义

## 🧪 运行测试

```bash
# 运行所有测试
python -m pytest sqlbuster/tests/ -v

# 运行测试并生成覆盖率报告
python -m pytest sqlbuster/tests/ --cov=sqlbuster --cov-report=html

# 运行特定测试文件
python -m pytest sqlbuster/tests/test_engine.py -v
```

### 测试代表性采样
测试文件包含专门针对代表性采样的测试用例：
- `TestRepresentativeSampling`：测试类型成功缓存和采样逻辑
- `TestWhereClauseGeneratorRepresentativeSampling`：测试WHERE生成器的采样行为

## 📁 项目结构

```
sqlbuster/
├── sqlbuster/
│   ├── __init__.py
│   ├── clauses/          # 子句生成器
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── cte.py        # CTE (WITH) 子句生成器
│   │   ├── select.py
│   │   ├── join.py       # JOIN 子句生成器
│   │   ├── where.py
│   │   ├── group_by.py
│   │   ├── having.py
│   │   ├── window.py     # WINDOW 子句生成器
│   │   ├── order_by.py
│   │   ├── limit.py
│   │   └── union.py      # UNION 子句生成器
│   ├── core/             # 核心模块
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   ├── registry.py
│   │   └── runner.py
│   ├── examples/         # 示例代码
│   │   ├── __init__.py
│   │   └── mock_runner_demo.py
│   ├── reporter/         # 报告生成
│   │   ├── __init__.py
│   │   └── reporter.py
│   ├── tests/            # 单元测试
│   │   │   ├── __init__.py
│   │   │   ├── test_engine.py
│   │   │   ├── test_registry.py
│   │   │   ├── test_runner.py
│   │   │   └── test_reporter.py
│   └── utils/            # 工具模块
│       ├── __init__.py
│       ├── errors.py
│       └── types.py
├── plans/                # 设计文档
│   └── architecture.md
├── requirements.txt      # 项目依赖
├── pyproject.toml       # 项目配置
├── README.md            # 项目说明 (英文)
├── README_CN.md         # 项目说明 (中文)
└── LICENSE              # 许可证
```

## 🔧 扩展开发

### 添加新的子句生成器

1. 在 `sqlbuster/clauses/` 目录下创建新的生成器文件
2. 继承 `BaseClauseGenerator` 类
3. 实现 `generate` 方法（支持可选的 `type_success_cache` 参数以实现代表性采样）
4. 使用 `register_clause_generator` 函数注册新的生成器

示例：
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
        # 实现你的生成逻辑
        # 可以利用 type_success_cache 实现代表性采样
        pass

# 注册生成器
register_clause_generator(engine, ClauseType.MY_CLAUSE, MyClauseGenerator, registry)
```

### 实现自定义 SQL 执行器

```python
from sqlbuster.core.runner import BaseSQLRunner

class MyCustomRunner(BaseSQLRunner):
    def execute_sql(self, sql: str) -> tuple[bool, str | None]:
        # 实现你的 SQL 执行逻辑
        # 返回 (成功与否, 错误信息)
        pass
    
    def get_supported_clauses(self) -> set[str]:
        # 返回支持的子句类型集合
        pass
```

## 📄 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题或建议，请通过以下方式联系：
- 提交 GitHub Issue
- 发送邮件至：[iamjrluo at outlook.com]

---

**注意**：本项目当前处于开发阶段，API 可能会发生变化。

## 🎯 性能对比

启用代表性采样优化后，对于包含多个同类型列的表，测试次数显著减少：

| 场景 | 优化前测试次数 | 优化后测试次数 | 减少比例 |
|------|----------------|----------------|----------|
| 3个int列，WHERE测试 | 3 | 1 | 66.7% |
| 5个str列，ORDER BY测试 | 10+ | 2 | 80%+ |

实际效果取决于表结构和列类型分布。
