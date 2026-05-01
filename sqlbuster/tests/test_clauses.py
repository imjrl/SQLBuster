"""
子句生成器单元测试

本模块测试所有子句生成器的功能，包括：
- BaseClauseGenerator 基类
- SelectClauseGenerator
- WhereClauseGenerator
- GroupByClauseGenerator
- HavingClauseGenerator
- OrderByClauseGenerator
- LimitClauseGenerator
"""

import unittest
from typing import Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator, register_clause_generator
from sqlbuster.clauses.group_by import GroupByClauseGenerator
from sqlbuster.clauses.having import HavingClauseGenerator
from sqlbuster.clauses.limit import LimitClauseGenerator
from sqlbuster.clauses.order_by import OrderByClauseGenerator
from sqlbuster.clauses.select import SelectClauseGenerator
from sqlbuster.clauses.where import WhereClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry, TableSchema
from sqlbuster.core.runner import MockRunner


class TestBaseClauseGenerator(unittest.TestCase):
    """测试BaseClauseGenerator基类"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int, is_nullable=False),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
            ColumnSchema(name="score", type=float),
        ]
        self.registry.register_table("users", self.columns)
        self.generator = SelectClauseGenerator(self.registry)

    def test_base_class_is_abstract(self) -> None:
        """测试BaseClauseGenerator是抽象类"""
        with self.assertRaises(TypeError):
            # 尝试实例化抽象类应该失败
            generator = BaseClauseGenerator(self.registry)  # type: ignore[abstract]

    def test_get_random_column_from_table(self) -> None:
        """测试get_random_column_from_table方法"""
        col = self.generator.get_random_column_from_table("users")
        self.assertIsNotNone(col)
        if col is not None:
            self.assertIn(col.name, ["id", "name", "age", "score"])

    def test_get_random_column_nonexistent_table(self) -> None:
        """测试获取不存在表的随机列"""
        col = self.generator.get_random_column_from_table("nonexistent")
        self.assertIsNone(col)

    def test_get_columns_from_table(self) -> None:
        """测试get_columns_from_table方法"""
        columns = self.generator.get_columns_from_table("users")
        self.assertEqual(len(columns), 4)

    def test_get_columns_nonexistent_table(self) -> None:
        """测试获取不存在表的列"""
        columns = self.generator.get_columns_from_table("nonexistent")
        self.assertEqual(len(columns), 0)

    def test_get_first_column(self) -> None:
        """测试get_first_column方法"""
        col = self.generator.get_first_column("users")
        self.assertIsNotNone(col)
        if col is not None:
            self.assertEqual(col.name, "id")

    def test_get_first_column_empty_table(self) -> None:
        """测试获取空表的第一列"""
        self.registry.register_table("empty", [])
        col = self.generator.get_first_column("empty")
        self.assertIsNone(col)


class TestSelectClauseGenerator(unittest.TestCase):
    """测试SelectClauseGenerator"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int, is_nullable=False),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
        ]
        self.registry.register_table("users", self.columns)
        self.generator = SelectClauseGenerator(self.registry)

    def test_generate_select_variants(self) -> None:
        """测试生成SELECT子句变体"""
        variants = self.generator.generate(ClauseType.SELECT, self.registry)
        self.assertGreater(len(variants), 0)

        # 检查生成的变体
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.SELECT)
            self.assertIn("SELECT", variant.sql_fragment)

    def test_generate_wrong_clause_type(self) -> None:
        """测试传入错误的子句类型"""
        variants = self.generator.generate(ClauseType.WHERE, self.registry)
        self.assertEqual(len(variants), 0)

    def test_variants_have_priority(self) -> None:
        """测试生成的变体有优先级"""
        variants = self.generator.generate(ClauseType.SELECT, self.registry)
        for variant in variants:
            self.assertIsNotNone(variant.priority)

    def test_variants_have_metadata(self) -> None:
        """测试生成的变体有元数据"""
        variants = self.generator.generate(ClauseType.SELECT, self.registry)
        for variant in variants:
            self.assertIsNotNone(variant.metadata)
            # SELECT变体的metadata包含type字段
            self.assertIn("type", variant.metadata)


class TestWhereClauseGenerator(unittest.TestCase):
    """测试WhereClauseGenerator"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int, is_nullable=False),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
            ColumnSchema(name="score", type=float),
            ColumnSchema(name="active", type=bool),
        ]
        self.registry.register_table("users", self.columns)
        self.generator = WhereClauseGenerator(self.registry)

    def test_generate_where_variants(self) -> None:
        """测试生成WHERE子句变体"""
        variants = self.generator.generate(ClauseType.WHERE, self.registry)
        self.assertGreater(len(variants), 0)

        # 检查生成的变体
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.WHERE)
            self.assertIn("WHERE", variant.sql_fragment)

    def test_generate_with_cache(self) -> None:
        """测试使用缓存生成WHERE变体（代表性采样）"""
        cache: Dict = {}
        # 模拟缓存命中：int类型已成功
        cache[("users", ClauseType.WHERE, int)] = True

        variants = self.generator.generate(
            ClauseType.WHERE, self.registry, type_success_cache=cache
        )

        # 使用缓存时：int类型只生成1个代表性变体
        # 其他类型（str, float, bool）未缓存，每种生成1个变体
        # 总共：1 (int) + 1 (str) + 1 (float) + 1 (bool) = 4个变体
        self.assertEqual(len(variants), 4)

        # 检查int类型的变体有representative标记
        int_variants = [
            v for v in variants if "id" in v.sql_fragment or "age" in v.sql_fragment
        ]
        self.assertGreater(len(int_variants), 0)
        for v in int_variants:
            self.assertTrue(v.metadata.get("representative", False))

        # 检查其他类型的变体没有representative标记
        other_variants = [v for v in variants if v not in int_variants]
        for v in other_variants:
            self.assertNotIn("representative", v.metadata)

    def test_generate_without_cache(self) -> None:
        """测试不使用缓存生成WHERE变体"""
        variants = self.generator.generate(ClauseType.WHERE, self.registry)

        # 不使用缓存时，应该为每列生成变体
        # 5列，每列1个变体 = 5个变体
        self.assertEqual(len(variants), 5)

        # 检查metadata中没有representative标记
        for variant in variants:
            self.assertNotIn("representative", variant.metadata)

    def test_where_condition_types(self) -> None:
        """测试WHERE条件的类型"""
        variants = self.generator.generate(ClauseType.WHERE, self.registry)

        # 收集所有条件
        conditions = [v.sql_fragment for v in variants]

        # 应该包含各种类型的条件
        str_conditions = [c for c in conditions if "name" in c]
        int_conditions = [c for c in conditions if "id" in c or "age" in c]
        float_conditions = [c for c in conditions if "score" in c]
        bool_conditions = [c for c in conditions if "active" in c]

        self.assertGreater(len(str_conditions), 0)
        self.assertGreater(len(int_conditions), 0)
        self.assertGreater(len(float_conditions), 0)
        self.assertGreater(len(bool_conditions), 0)


class TestGroupByClauseGenerator(unittest.TestCase):
    """测试GroupByClauseGenerator"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
        ]
        self.registry.register_table("users", self.columns)
        self.generator = GroupByClauseGenerator(self.registry)

    def test_generate_group_by_variants(self) -> None:
        """测试生成GROUP BY子句变体"""
        variants = self.generator.generate(ClauseType.GROUP_BY, self.registry)
        self.assertGreater(len(variants), 0)

        # 检查生成的变体
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.GROUP_BY)
            self.assertIn("GROUP BY", variant.sql_fragment)

    def test_generate_with_cache(self) -> None:
        """测试使用缓存生成GROUP BY变体"""
        cache: Dict = {}
        cache[("users", ClauseType.GROUP_BY, int)] = True
        cache[("users", ClauseType.GROUP_BY, str)] = True

        variants = self.generator.generate(
            ClauseType.GROUP_BY, self.registry, type_success_cache=cache
        )

        # 使用缓存时，每种类型只生成一个代表性变体
        # int和str两种类型 = 2个单列强体 + 可能的多列变体
        self.assertGreaterEqual(len(variants), 2)

    def test_generate_without_cache_iterates_all_columns(self) -> None:
        """测试不使用缓存时迭代所有列"""
        variants = self.generator.generate(ClauseType.GROUP_BY, self.registry)

        # 不使用缓存时，应该为每列生成变体
        # 3列 = 3个单列强体 + 多列变体(如果列数>=2)
        self.assertGreaterEqual(len(variants), 3)

    def test_multi_column_group_by(self) -> None:
        """测试多列GROUP BY"""
        variants = self.generator.generate(ClauseType.GROUP_BY, self.registry)

        # 检查是否有多列GROUP BY
        multi_col = [v for v in variants if "," in v.sql_fragment]
        self.assertGreater(len(multi_col), 0)


class TestHavingClauseGenerator(unittest.TestCase):
    """测试HavingClauseGenerator"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="score", type=float),
        ]
        self.registry.register_table("users", self.columns)
        self.generator = HavingClauseGenerator(self.registry)

    def test_generate_having_variants(self) -> None:
        """测试生成HAVING子句变体"""
        variants = self.generator.generate(ClauseType.HAVING, self.registry)
        self.assertGreater(len(variants), 0)

        # 检查生成的变体
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.HAVING)
            self.assertIn("HAVING", variant.sql_fragment)

    def test_having_count_always_present(self) -> None:
        """测试HAVING COUNT(*)总是生成"""
        variants = self.generator.generate(ClauseType.HAVING, self.registry)

        count_variants = [v for v in variants if "COUNT" in v.sql_fragment]
        self.assertGreater(len(count_variants), 0)

    def test_having_with_numeric_columns(self) -> None:
        """测试数值列的HAVING聚合条件"""
        variants = self.generator.generate(ClauseType.HAVING, self.registry)

        # id (int) 和 score (float) 应该生成SUM, AVG, MAX, MIN
        agg_variants = [
            v
            for v in variants
            if any(f in v.sql_fragment for f in ["SUM", "AVG", "MAX", "MIN"])
        ]
        self.assertGreater(len(agg_variants), 0)


class TestOrderByClauseGenerator(unittest.TestCase):
    """测试OrderByClauseGenerator"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
        ]
        self.registry.register_table("users", self.columns)
        self.generator = OrderByClauseGenerator(self.registry)

    def test_generate_order_by_variants(self) -> None:
        """测试生成ORDER BY子句变体"""
        variants = self.generator.generate(ClauseType.ORDER_BY, self.registry)
        self.assertGreater(len(variants), 0)

        # 检查生成的变体
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.ORDER_BY)
            self.assertIn("ORDER BY", variant.sql_fragment)

    def test_order_by_asc_and_desc(self) -> None:
        """测试ORDER BY升序和降序"""
        variants = self.generator.generate(ClauseType.ORDER_BY, self.registry)

        asc_variants = [v for v in variants if "DESC" not in v.sql_fragment]
        desc_variants = [v for v in variants if "DESC" in v.sql_fragment]

        self.assertGreater(len(asc_variants), 0)
        self.assertGreater(len(desc_variants), 0)

    def test_multi_column_order_by(self) -> None:
        """测试多列ORDER BY"""
        variants = self.generator.generate(ClauseType.ORDER_BY, self.registry)

        # 如果有2+列，应该有多列排序
        multi_col = [v for v in variants if "," in v.sql_fragment]
        if len(self.columns) >= 2:
            self.assertGreater(len(multi_col), 0)


class TestLimitClauseGenerator(unittest.TestCase):
    """测试LimitClauseGenerator"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.generator = LimitClauseGenerator(self.registry)

    def test_generate_limit_variants(self) -> None:
        """测试生成LIMIT子句变体"""
        variants = self.generator.generate(ClauseType.LIMIT, self.registry)
        self.assertGreater(len(variants), 0)

        # 检查生成的变体
        for variant in variants:
            self.assertEqual(variant.clause_type, ClauseType.LIMIT)
            self.assertIn("LIMIT", variant.sql_fragment)

    def test_limit_values(self) -> None:
        """测试LIMIT的值"""
        variants = self.generator.generate(ClauseType.LIMIT, self.registry)

        limit_values = [v.sql_fragment for v in variants]
        self.assertIn("LIMIT 10", limit_values)
        self.assertIn("LIMIT 1", limit_values)

    def test_limit_with_offset(self) -> None:
        """测试LIMIT with OFFSET"""
        variants = self.generator.generate(ClauseType.LIMIT, self.registry)

        offset_variants = [v for v in variants if "OFFSET" in v.sql_fragment]
        self.assertGreater(len(offset_variants), 0)

    def test_limit_does_not_use_registry(self) -> None:
        """测试LIMIT不依赖registry"""
        # 即使registry为空，LIMIT也能生成变体
        empty_registry = SchemaRegistry()
        generator = LimitClauseGenerator(empty_registry)
        variants = generator.generate(ClauseType.LIMIT, empty_registry)
        self.assertGreater(len(variants), 0)


class TestRegisterClauseGenerator(unittest.TestCase):
    """测试register_clause_generator函数"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.registry = SchemaRegistry()
        self.columns = [
            ColumnSchema(name="id", type=int),
        ]
        self.registry.register_table("users", self.columns)
        self.runner = MockRunner()

        # 延迟导入EvolvingEngine避免循环导入
        from sqlbuster.core.engine import EvolvingEngine

        self.engine = EvolvingEngine(
            registry=self.registry, runner=self.runner, start_table="users"
        )

    def test_register_clause_generator(self) -> None:
        """测试注册子句生成器"""
        # 注册一个生成器
        register_clause_generator(
            self.engine, ClauseType.SELECT, SelectClauseGenerator, self.registry
        )

        # 检查是否注册成功
        # 通过_generate_clause_variants方法检查
        self.assertIsNotNone(self.engine)
        if self.engine is not None:
            variants = self.engine._generate_clause_variants(ClauseType.SELECT)
            self.assertGreater(len(variants), 0)

    def test_register_multiple_generators(self) -> None:
        """测试注册多个子句生成器"""
        register_clause_generator(
            self.engine, ClauseType.SELECT, SelectClauseGenerator, self.registry
        )
        register_clause_generator(
            self.engine, ClauseType.WHERE, WhereClauseGenerator, self.registry
        )

        # 检查是否都注册成功
        self.assertIsNotNone(self.engine)
        if self.engine is not None:
            select_variants = self.engine._generate_clause_variants(ClauseType.SELECT)
            where_variants = self.engine._generate_clause_variants(ClauseType.WHERE)

            self.assertGreater(len(select_variants), 0)
            self.assertGreater(len(where_variants), 0)


if __name__ == "__main__":
    unittest.main()
