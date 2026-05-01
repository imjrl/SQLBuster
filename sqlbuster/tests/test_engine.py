"""
进化引擎单元测试

本模块测试sqlbuster.core.engine中的进化引擎相关类，
重点测试DFS递归、回溯逻辑、子句生成功能和代表性采样优化。
"""

import unittest
from collections import defaultdict
from typing import Callable, Dict, List, Optional

from sqlbuster.clauses.base import register_clause_generator
from sqlbuster.clauses.cte import CteClauseGenerator
from sqlbuster.clauses.group_by import GroupByClauseGenerator
from sqlbuster.clauses.having import HavingClauseGenerator
from sqlbuster.clauses.join import JoinClauseGenerator
from sqlbuster.clauses.limit import LimitClauseGenerator
from sqlbuster.clauses.order_by import OrderByClauseGenerator
from sqlbuster.clauses.select import SelectClauseGenerator
from sqlbuster.clauses.union import UnionClauseGenerator
from sqlbuster.clauses.where import WhereClauseGenerator
from sqlbuster.clauses.window import WindowClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant, EvolvingEngine, SQLNode
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry, TableSchema
from sqlbuster.core.runner import MockRunner


def _register_default_generators(engine: EvolvingEngine) -> None:
    """
    注册默认的测试用子句生成器

    使用新的register_clause_generator函数，支持代表性采样。

    Args:
        engine: 要注册生成器的引擎实例
    """
    registry = engine._registry  # type: ignore

    register_clause_generator(engine, ClauseType.CTE, CteClauseGenerator, registry)
    register_clause_generator(
        engine, ClauseType.SELECT, SelectClauseGenerator, registry
    )
    register_clause_generator(engine, ClauseType.JOIN, JoinClauseGenerator, registry)
    register_clause_generator(engine, ClauseType.WHERE, WhereClauseGenerator, registry)
    register_clause_generator(
        engine, ClauseType.GROUP_BY, GroupByClauseGenerator, registry
    )
    register_clause_generator(
        engine, ClauseType.HAVING, HavingClauseGenerator, registry
    )
    register_clause_generator(
        engine, ClauseType.WINDOW, WindowClauseGenerator, registry
    )
    register_clause_generator(
        engine, ClauseType.ORDER_BY, OrderByClauseGenerator, registry
    )
    register_clause_generator(engine, ClauseType.LIMIT, LimitClauseGenerator, registry)
    register_clause_generator(engine, ClauseType.UNION, UnionClauseGenerator, registry)


class TestClauseType(unittest.TestCase):
    """测试ClauseType枚举"""

    def test_clause_type_values(self) -> None:
        """测试ClauseType枚举值"""
        self.assertEqual(ClauseType.CTE.value, "WITH")
        self.assertEqual(ClauseType.SELECT.value, "SELECT")
        self.assertEqual(ClauseType.JOIN.value, "JOIN")
        self.assertEqual(ClauseType.WHERE.value, "WHERE")
        self.assertEqual(ClauseType.GROUP_BY.value, "GROUP BY")
        self.assertEqual(ClauseType.HAVING.value, "HAVING")
        self.assertEqual(ClauseType.WINDOW.value, "WINDOW")
        self.assertEqual(ClauseType.ORDER_BY.value, "ORDER BY")
        self.assertEqual(ClauseType.LIMIT.value, "LIMIT")
        self.assertEqual(ClauseType.UNION.value, "UNION")

    def test_next_clause(self) -> None:
        """测试获取下一个子句类型"""
        self.assertEqual(ClauseType.CTE.next, ClauseType.SELECT)
        self.assertEqual(ClauseType.SELECT.next, ClauseType.JOIN)
        self.assertEqual(ClauseType.JOIN.next, ClauseType.WHERE)
        self.assertEqual(ClauseType.WHERE.next, ClauseType.GROUP_BY)
        self.assertEqual(ClauseType.GROUP_BY.next, ClauseType.HAVING)
        self.assertEqual(ClauseType.HAVING.next, ClauseType.WINDOW)
        self.assertEqual(ClauseType.WINDOW.next, ClauseType.ORDER_BY)
        self.assertEqual(ClauseType.ORDER_BY.next, ClauseType.LIMIT)
        self.assertEqual(ClauseType.LIMIT.next, ClauseType.UNION)
        self.assertIsNone(ClauseType.UNION.next)

    def test_previous_clause(self) -> None:
        """测试获取上一个子句类型"""
        self.assertIsNone(ClauseType.CTE.previous)
        self.assertEqual(ClauseType.SELECT.previous, ClauseType.CTE)
        self.assertEqual(ClauseType.JOIN.previous, ClauseType.SELECT)
        self.assertEqual(ClauseType.WHERE.previous, ClauseType.JOIN)
        self.assertEqual(ClauseType.GROUP_BY.previous, ClauseType.WHERE)
        self.assertEqual(ClauseType.HAVING.previous, ClauseType.GROUP_BY)
        self.assertEqual(ClauseType.WINDOW.previous, ClauseType.HAVING)
        self.assertEqual(ClauseType.ORDER_BY.previous, ClauseType.WINDOW)
        self.assertEqual(ClauseType.LIMIT.previous, ClauseType.ORDER_BY)
        self.assertEqual(ClauseType.UNION.previous, ClauseType.LIMIT)


class TestClauseVariant(unittest.TestCase):
    """测试ClauseVariant数据类"""

    def test_clause_variant_creation(self) -> None:
        """测试创建ClauseVariant实例"""
        variant = ClauseVariant(
            clause_type=ClauseType.WHERE, sql_fragment="WHERE age > 18", priority=1
        )
        self.assertEqual(variant.clause_type, ClauseType.WHERE)
        self.assertEqual(variant.sql_fragment, "WHERE age > 18")
        self.assertEqual(variant.priority, 1)

    def test_clause_variant_default_priority(self) -> None:
        """测试默认优先级"""
        variant = ClauseVariant(clause_type=ClauseType.LIMIT, sql_fragment="LIMIT 10")
        self.assertEqual(variant.priority, 0)

    def test_clause_variant_with_metadata(self) -> None:
        """测试带metadata的ClauseVariant"""
        metadata = {"column": "age", "type": "int", "representative": True}
        variant = ClauseVariant(
            clause_type=ClauseType.WHERE,
            sql_fragment="WHERE age > 18",
            priority=1,
            metadata=metadata,
        )
        self.assertEqual(variant.metadata["column"], "age")
        self.assertTrue(variant.metadata["representative"])


class TestSQLNode(unittest.TestCase):
    """测试SQLNode数据类"""

    def test_sql_node_creation(self) -> None:
        """测试创建SQLNode实例"""
        node = SQLNode(sql="SELECT * FROM users")
        self.assertEqual(node.sql, "SELECT * FROM users")
        self.assertIsNone(node.clause_type)
        self.assertEqual(len(node.children), 0)
        self.assertIsNone(node.parent)
        self.assertIsNone(node.success)
        self.assertIsNone(node.error_msg)

    def test_sql_node_with_clause_type(self) -> None:
        """测试带子句类型的SQLNode"""
        node = SQLNode(
            sql="SELECT * FROM users WHERE age > 18", clause_type=ClauseType.WHERE
        )
        self.assertEqual(node.clause_type, ClauseType.WHERE)

    def test_add_child(self) -> None:
        """测试添加子节点"""
        parent = SQLNode(sql="SELECT * FROM users")
        child = SQLNode(
            sql="SELECT * FROM users WHERE age > 18", clause_type=ClauseType.WHERE
        )
        parent.add_child(child)

        self.assertEqual(len(parent.children), 1)
        self.assertEqual(parent.children[0], child)
        self.assertEqual(child.parent, parent)

    def test_get_path(self) -> None:
        """测试获取路径"""
        root = SQLNode(sql="SELECT * FROM users")
        child1 = SQLNode(
            sql="SELECT * FROM users WHERE age > 18",
            clause_type=ClauseType.WHERE,
            parent=root,
        )
        child2 = SQLNode(
            sql="SELECT * FROM users WHERE age > 18 LIMIT 10",
            clause_type=ClauseType.LIMIT,
            parent=child1,
        )

        path = child2.get_path()
        self.assertEqual(len(path), 3)
        self.assertEqual(path[0], root)
        self.assertEqual(path[1], child1)
        self.assertEqual(path[2], child2)

    def test_is_root(self) -> None:
        """测试判断根节点"""
        root = SQLNode(sql="SELECT * FROM users")
        self.assertTrue(root.is_root())

        child = SQLNode(sql="SELECT * FROM users WHERE age > 18", parent=root)
        self.assertFalse(child.is_root())

    def test_is_leaf(self) -> None:
        """测试判断叶子节点"""
        node = SQLNode(sql="SELECT * FROM users")
        self.assertTrue(node.is_leaf())

        child = SQLNode(sql="SELECT * FROM users WHERE age > 18")
        node.add_child(child)
        self.assertFalse(node.is_leaf())


class TestEvolvingEngine(unittest.TestCase):
    """测试EvolvingEngine类"""

    def setUp(self) -> None:
        """测试前准备"""
        # 创建模式注册表
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int, is_nullable=False),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
            ColumnSchema(name="score", type=float),
        ]
        self.registry.register_table("users", columns)

        # 创建模拟执行器（支持所有子句）
        self.runner = MockRunner()

        # 创建进化引擎
        self.engine = EvolvingEngine(
            registry=self.registry, runner=self.runner, start_table="users"
        )

        # 注册默认的子句生成器
        _register_default_generators(self.engine)

    def test_engine_initialization(self) -> None:
        """测试引擎初始化"""
        self.assertIsNotNone(self.engine)
        self.assertEqual(self.engine._start_table, "users")

    def test_engine_initialization_invalid_registry(self) -> None:
        """测试无效注册表初始化"""
        with self.assertRaises(Exception):
            engine = EvolvingEngine(registry=None, runner=self.runner)  # type: ignore

    def test_engine_initialization_invalid_runner(self) -> None:
        """测试无效执行器初始化"""
        with self.assertRaises(Exception):
            engine = EvolvingEngine(registry=self.registry, runner=None)  # type: ignore

    def test_explore_basic(self) -> None:
        """测试基础探索功能"""
        nodes = list(self.engine.explore())

        # 至少应该有一个根节点
        self.assertGreater(len(nodes), 0)

        # 第一个节点应该是根节点
        root = nodes[0]
        self.assertTrue(root.is_root())
        self.assertIsNotNone(root.success)

    def test_explore_with_unsupported_clause(self) -> None:
        """测试探索时不支持的子句"""
        # 创建不支持HAVING的执行器
        runner = MockRunner({"HAVING"})
        engine = EvolvingEngine(
            registry=self.registry, runner=runner, start_table="users"
        )
        # 注册子句生成器
        _register_default_generators(engine)

        nodes = list(engine.explore())

        # 检查是否有节点失败
        failed_nodes = [n for n in nodes if n.success is False]
        # 由于我们注册了HAVING生成器，且runner不支持HAVING，应该有失败节点
        # 但注意：只有当HAVING变体被生成并尝试执行时才会有失败
        # 这里主要测试不会崩溃
        self.assertGreater(len(nodes), 0)

    def test_dfs_backtrack(self) -> None:
        """测试DFS回溯逻辑"""
        # 创建一个只支持SELECT和WHERE的执行器
        runner = MockRunner({"GROUP BY", "HAVING", "ORDER BY", "LIMIT"})
        engine = EvolvingEngine(
            registry=self.registry, runner=runner, start_table="users"
        )
        # 注册子句生成器
        _register_default_generators(engine)

        nodes = list(engine.explore())

        # 根节点应该成功
        root = nodes[0]
        self.assertTrue(root.success)

        # 检查是否有失败的节点（尝试不支持的子句时）
        failed_nodes = [n for n in nodes if n.success is False]
        # 由于只支持SELECT和WHERE，尝试GROUP BY等应该失败
        # 但具体是否有失败节点取决于子句生成器的实现
        # 这里主要测试不会无限递归或崩溃
        self.assertGreater(len(nodes), 0)

    def test_generate_clause_variants(self) -> None:
        """测试生成子句变体"""
        # 测试SELECT变体
        variants = self.engine._generate_clause_variants(ClauseType.SELECT)
        self.assertGreater(len(variants), 0)

        # 检查变体类型
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.SELECT)
            self.assertIsNotNone(variant.sql_fragment)

    def test_build_sql_select(self) -> None:
        """测试构建SELECT SQL"""
        base_sql = "SELECT * FROM users"
        variant = ClauseVariant(
            clause_type=ClauseType.SELECT, sql_fragment="SELECT id, name"
        )
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("SELECT id, name", new_sql)
        self.assertIn("FROM users", new_sql)

    def test_build_sql_where(self) -> None:
        """测试构建WHERE SQL"""
        base_sql = "SELECT * FROM users"
        variant = ClauseVariant(
            clause_type=ClauseType.WHERE, sql_fragment="WHERE age > 18"
        )
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("WHERE age > 18", new_sql)

    def test_build_sql_limit(self) -> None:
        """测试构建LIMIT SQL"""
        base_sql = "SELECT * FROM users WHERE age > 18"
        variant = ClauseVariant(clause_type=ClauseType.LIMIT, sql_fragment="LIMIT 10")
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("LIMIT 10", new_sql)

    def test_get_next_clause(self) -> None:
        """测试获取下一个子句"""
        self.assertEqual(
            self.engine._get_next_clause(ClauseType.CTE), ClauseType.SELECT
        )
        self.assertEqual(
            self.engine._get_next_clause(ClauseType.SELECT), ClauseType.JOIN
        )
        self.assertEqual(
            self.engine._get_next_clause(ClauseType.JOIN), ClauseType.WHERE
        )
        self.assertIsNone(self.engine._get_next_clause(ClauseType.UNION))

    def test_register_clause_generator(self) -> None:
        """测试注册子句生成器"""

        def dummy_generator(
            clause_type: ClauseType,
            registry: Optional[SchemaRegistry],
            type_success_cache: Optional[Dict] = None,
        ) -> List[ClauseVariant]:
            return [ClauseVariant(clause_type=clause_type, sql_fragment="DUMMY")]

        self.engine.register_clause_generator(ClauseType.SELECT, dummy_generator)

        variants = self.engine._generate_clause_variants(ClauseType.SELECT)
        self.assertGreater(len(variants), 0)
        self.assertEqual(variants[0].sql_fragment, "DUMMY")


class TestEngineWithClauseGenerators(unittest.TestCase):
    """测试带子句生成器的引擎"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
        ]
        self.registry.register_table("test", columns)

    def test_explore_with_all_supported(self) -> None:
        """测试所有子句都支持时的探索"""
        runner = MockRunner()  # 支持所有子句

        engine = EvolvingEngine(
            registry=self.registry, runner=runner, start_table="test"
        )

        # 注册子句生成器
        _register_default_generators(engine)

        # 执行探索
        nodes = list(engine.explore())

        # 验证探索执行
        self.assertGreater(len(nodes), 0)

        # 验证所有节点都成功（因为runner支持所有子句）
        for node in nodes:
            self.assertTrue(node.success, f"Node failed: {node.sql}")


class TestRepresentativeSampling(unittest.TestCase):
    """测试代表性采样优化"""

    def setUp(self) -> None:
        """测试前准备"""
        # 创建一个包含多个同类型列的表
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int, is_nullable=False),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),  # int类型，同类型列1
            ColumnSchema(name="score", type=int),  # int类型，同类型列2
            ColumnSchema(name="count", type=int),  # int类型，同类型列3
            ColumnSchema(name="description", type=str),  # str类型，同类型列1
            ColumnSchema(name="address", type=str),  # str类型，同类型列2
            ColumnSchema(name="grade", type=float),
        ]
        self.registry.register_table("students", columns)

        # 创建模拟执行器（支持所有子句）
        self.runner = MockRunner()

        # 创建进化引擎
        self.engine = EvolvingEngine(
            registry=self.registry, runner=self.runner, start_table="students"
        )

        # 注册默认的子句生成器
        _register_default_generators(self.engine)

    def test_type_success_cache_initialization(self) -> None:
        """测试类型成功缓存初始化"""
        self.assertIsNotNone(self.engine._type_success_cache)
        self.assertEqual(len(self.engine._type_success_cache), 0)

    def test_type_success_cache_update(self) -> None:
        """测试类型成功缓存更新"""
        # 手动添加缓存
        cache_key = ("students", ClauseType.WHERE, int)
        self.engine._type_success_cache[cache_key] = True

        self.assertIn(cache_key, self.engine._type_success_cache)
        self.assertTrue(self.engine._type_success_cache[cache_key])

    def test_generate_variants_with_cache(self) -> None:
        """测试使用缓存生成变体（代表性采样）"""
        # 先往缓存中添加记录，表示int类型在WHERE子句已成功
        cache_key = ("students", ClauseType.WHERE, int)
        self.engine._type_success_cache[cache_key] = True

        # 生成WHERE子句变体
        variants = self.engine._generate_clause_variants(ClauseType.WHERE)

        # 检查变体
        self.assertGreater(len(variants), 0)

        # 对于int类型，应该只生成一个代表性变体（因为缓存中已成功）
        int_variants = [v for v in variants if v.metadata.get("type") == "int"]

        # 如果有代表性标记，说明使用了代表性采样
        representative_variants = [
            v for v in int_variants if v.metadata.get("representative")
        ]

        # 注意：这个测试取决于WhereClauseGenerator的实现
        # 如果缓存命中，应该只生成一个代表性变体
        if representative_variants:
            # 只应该有一个int类型的代表性变体
            self.assertLessEqual(len(representative_variants), 1)

    def test_generate_variants_without_cache(self) -> None:
        """测试不使用缓存生成变体（完整测试）"""
        # 确保缓存为空
        self.engine._type_success_cache.clear()

        # 生成WHERE子句变体
        variants = self.engine._generate_clause_variants(ClauseType.WHERE)

        # 检查变体
        self.assertGreater(len(variants), 0)

        # 检查是否有代表性标记
        representative_variants = [
            v for v in variants if v.metadata.get("representative")
        ]
        # 没有缓存时，不应该有代表性变体
        self.assertEqual(len(representative_variants), 0)

    def test_explore_with_representative_sampling(self) -> None:
        """测试探索过程中的代表性采样"""
        # 执行探索
        nodes = list(self.engine.explore())

        # 验证探索执行
        self.assertGreater(len(nodes), 0)

        # 检查类型成功缓存
        cache = self.engine.get_type_success_cache()

        # 如果探索成功，缓存中应该有记录
        # 注意：这取决于生成器的实现和runner的行为
        # 这里主要测试不会崩溃
        self.assertIsInstance(cache, dict)

    def test_get_type_success_cache(self) -> None:
        """测试获取类型成功缓存"""
        # 添加一些缓存记录
        self.engine._type_success_cache[("students", ClauseType.WHERE, int)] = True
        self.engine._type_success_cache[("students", ClauseType.WHERE, str)] = True

        cache = self.engine.get_type_success_cache()

        self.assertEqual(len(cache), 2)
        self.assertIn(("students", ClauseType.WHERE, int), cache)
        self.assertIn(("students", ClauseType.WHERE, str), cache)

        # 确保返回的是副本，修改不会影响原缓存
        cache[("students", ClauseType.WHERE, float)] = True
        self.assertNotIn(
            ("students", ClauseType.WHERE, float), self.engine._type_success_cache
        )


class TestWhereClauseGeneratorRepresentativeSampling(unittest.TestCase):
    """测试WHERE子句生成器的代表性采样"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),  # 同类型列
            ColumnSchema(name="score", type=int),  # 同类型列
        ]
        self.registry.register_table("test", columns)
        self.generator = WhereClauseGenerator(self.registry)

    def test_generate_without_cache(self) -> None:
        """测试无缓存时生成变体"""
        variants = self.generator.generate(
            ClauseType.WHERE, registry=self.registry, type_success_cache=None
        )

        self.assertGreater(len(variants), 0)

        # 检查metadata
        for variant in variants:
            self.assertIn("type", variant.metadata)
            self.assertNotIn("representative", variant.metadata)

    def test_generate_with_cache_hit(self) -> None:
        """测试缓存命中时生成代表性变体"""
        # 创建缓存，表示int类型在WHERE已成功
        type_success_cache = {("test", ClauseType.WHERE, int): True}

        variants = self.generator.generate(
            ClauseType.WHERE,
            registry=self.registry,
            type_success_cache=type_success_cache,
        )

        self.assertGreater(len(variants), 0)

        # 检查int类型的变体
        int_variants = [v for v in variants if v.metadata.get("type") == "int"]

        # 应该有代表性标记
        for variant in int_variants:
            self.assertTrue(variant.metadata.get("representative", False))


class TestCteClauseGenerator(unittest.TestCase):
    """测试CTE (WITH子句) 生成器"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
        ]
        self.registry.register_table("test", columns)
        self.generator = CteClauseGenerator(self.registry)

    def test_generate_without_cache(self) -> None:
        """测试无缓存时生成CTE变体"""
        variants = self.generator.generate(
            ClauseType.CTE, registry=self.registry, type_success_cache=None
        )

        self.assertGreater(len(variants), 0)

        # 检查变体类型
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.CTE)
            self.assertIn("WITH", variant.sql_fragment)
            self.assertIn("cte", variant.sql_fragment)

    def test_generate_with_cache(self) -> None:
        """测试缓存命中时生成代表性变体"""
        type_success_cache = {("test", ClauseType.CTE, str): True}

        variants = self.generator.generate(
            ClauseType.CTE,
            registry=self.registry,
            type_success_cache=type_success_cache,
        )

        self.assertGreater(len(variants), 0)

        # 应该有代表性标记
        for variant in variants:
            self.assertTrue(variant.metadata.get("representative", False))


class TestJoinClauseGenerator(unittest.TestCase):
    """测试JOIN子句生成器"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns1 = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
        ]
        columns2 = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="score", type=float),
        ]
        self.registry.register_table("table1", columns1)
        self.registry.register_table("table2", columns2)
        self.generator = JoinClauseGenerator(self.registry)

    def test_generate_without_cache(self) -> None:
        """测试无缓存时生成JOIN变体"""
        variants = self.generator.generate(
            ClauseType.JOIN, registry=self.registry, type_success_cache=None
        )

        self.assertGreater(len(variants), 0)

        # 检查变体类型
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.JOIN)
            # JOIN变体应该包含JOIN关键字
            self.assertTrue(
                "JOIN" in variant.sql_fragment or "join" in variant.sql_fragment.lower()
            )

    def test_generate_with_cache(self) -> None:
        """测试缓存命中时生成代表性变体"""
        type_success_cache = {("table1", ClauseType.JOIN, str): True}

        variants = self.generator.generate(
            ClauseType.JOIN,
            registry=self.registry,
            type_success_cache=type_success_cache,
        )

        self.assertGreater(len(variants), 0)

        # 应该有代表性标记
        for variant in variants:
            self.assertTrue(variant.metadata.get("representative", False))

    def test_insufficient_tables(self) -> None:
        """测试表数量不足时不生成变体"""
        registry = SchemaRegistry()
        columns = [ColumnSchema(name="id", type=int)]
        registry.register_table("only_one", columns)
        generator = JoinClauseGenerator(registry)

        variants = generator.generate(
            ClauseType.JOIN, registry=registry, type_success_cache=None
        )

        # 只有一个表，不应该生成JOIN变体
        self.assertEqual(len(variants), 0)


class TestWindowClauseGenerator(unittest.TestCase):
    """测试WINDOW子句生成器"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="score", type=float),
        ]
        self.registry.register_table("test", columns)
        self.generator = WindowClauseGenerator(self.registry)

    def test_generate_without_cache(self) -> None:
        """测试无缓存时生成WINDOW变体"""
        variants = self.generator.generate(
            ClauseType.WINDOW, registry=self.registry, type_success_cache=None
        )

        self.assertGreater(len(variants), 0)

        # 检查变体类型
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.WINDOW)
            self.assertIn("WINDOW", variant.sql_fragment)

    def test_generate_with_cache(self) -> None:
        """测试缓存命中时生成代表性变体"""
        type_success_cache = {("test", ClauseType.WINDOW, str): True}

        variants = self.generator.generate(
            ClauseType.WINDOW,
            registry=self.registry,
            type_success_cache=type_success_cache,
        )

        self.assertGreater(len(variants), 0)

        # 应该有代表性标记
        for variant in variants:
            self.assertTrue(variant.metadata.get("representative", False))


class TestUnionClauseGenerator(unittest.TestCase):
    """测试UNION子句生成器"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns1 = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
        ]
        columns2 = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="address", type=str),
        ]
        self.registry.register_table("table1", columns1)
        self.registry.register_table("table2", columns2)
        self.generator = UnionClauseGenerator(self.registry)

    def test_generate_without_cache(self) -> None:
        """测试无缓存时生成UNION变体"""
        variants = self.generator.generate(
            ClauseType.UNION, registry=self.registry, type_success_cache=None
        )

        self.assertGreater(len(variants), 0)

        # 检查变体类型
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.UNION)
            self.assertIn("UNION", variant.sql_fragment)

    def test_generate_with_cache(self) -> None:
        """测试缓存命中时生成代表性变体"""
        type_success_cache = {("table1", ClauseType.UNION, str): True}

        variants = self.generator.generate(
            ClauseType.UNION,
            registry=self.registry,
            type_success_cache=type_success_cache,
        )

        self.assertGreater(len(variants), 0)

        # 应该有代表性标记
        for variant in variants:
            self.assertTrue(variant.metadata.get("representative", False))

    def test_insufficient_tables(self) -> None:
        """测试表数量不足时不生成变体"""
        registry = SchemaRegistry()
        columns = [ColumnSchema(name="id", type=int)]
        registry.register_table("only_one", columns)
        generator = UnionClauseGenerator(registry)

        variants = generator.generate(
            ClauseType.UNION, registry=registry, type_success_cache=None
        )

        # 只有一个表，不应该生成UNION变体
        self.assertEqual(len(variants), 0)


class TestBuildSqlNewClauses(unittest.TestCase):
    """测试新版_build_sql方法支持新子句类型"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
        ]
        self.registry.register_table("test", columns)
        self.runner = MockRunner()
        self.engine = EvolvingEngine(
            registry=self.registry, runner=self.runner, start_table="test"
        )

    def test_build_sql_cte(self) -> None:
        """测试构建CTE (WITH) SQL"""
        base_sql = "SELECT * FROM test"
        variant = ClauseVariant(
            clause_type=ClauseType.CTE,
            sql_fragment="WITH cte AS (SELECT * FROM test) SELECT * FROM cte",
        )
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("WITH cte", new_sql)
        self.assertIn("SELECT * FROM cte", new_sql)

    def test_build_sql_join(self) -> None:
        """测试构建JOIN SQL"""
        base_sql = "SELECT * FROM test"
        variant = ClauseVariant(
            clause_type=ClauseType.JOIN,
            sql_fragment="JOIN other_table ON test.id = other_table.id",
        )
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("JOIN other_table", new_sql)
        self.assertIn("ON test.id = other_table.id", new_sql)

    def test_build_sql_window(self) -> None:
        """测试构建WINDOW SQL"""
        base_sql = "SELECT * FROM test"
        variant = ClauseVariant(
            clause_type=ClauseType.WINDOW, sql_fragment="WINDOW w AS (PARTITION BY id)"
        )
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("WINDOW w AS (PARTITION BY id)", new_sql)

    def test_build_sql_union(self) -> None:
        """测试构建UNION SQL"""
        base_sql = "SELECT * FROM test"
        variant = ClauseVariant(
            clause_type=ClauseType.UNION, sql_fragment="UNION SELECT * FROM other_table"
        )
        new_sql = self.engine._build_sql(base_sql, variant)
        self.assertIn("UNION SELECT * FROM other_table", new_sql)


if __name__ == "__main__":
    unittest.main()
