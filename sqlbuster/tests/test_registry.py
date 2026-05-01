"""
SchemaRegistry单元测试

本模块测试sqlbuster.core.registry中的SchemaRegistry类及其相关数据结构。
"""

import unittest
from typing import List

from sqlbuster.core.registry import ColumnSchema, SchemaRegistry, TableSchema
from sqlbuster.utils.errors import ColumnNotFoundError, TableNotFoundError


class TestColumnSchema(unittest.TestCase):
    """测试ColumnSchema数据类"""

    def test_column_schema_creation(self) -> None:
        """测试创建ColumnSchema实例"""
        col = ColumnSchema(name="id", type=int, is_nullable=False)
        self.assertEqual(col.name, "id")
        self.assertEqual(col.type, int)
        self.assertFalse(col.is_nullable)

    def test_column_schema_default_nullable(self) -> None:
        """测试ColumnSchema默认值"""
        col = ColumnSchema(name="name", type=str)
        self.assertTrue(col.is_nullable)


class TestTableSchema(unittest.TestCase):
    """测试TableSchema数据类"""

    def test_table_schema_creation(self) -> None:
        """测试创建TableSchema实例"""
        columns = [
            ColumnSchema(name="id", type=int),
            ColumnSchema(name="name", type=str),
        ]
        table = TableSchema(name="users", columns=columns)
        self.assertEqual(table.name, "users")
        self.assertEqual(len(table.columns), 2)
        self.assertEqual(table.columns[0].name, "id")


class TestSchemaRegistry(unittest.TestCase):
    """测试SchemaRegistry类"""

    def setUp(self) -> None:
        """测试前准备"""
        self.registry = SchemaRegistry()
        self.columns: List[ColumnSchema] = [
            ColumnSchema(name="id", type=int, is_nullable=False),
            ColumnSchema(name="name", type=str),
            ColumnSchema(name="age", type=int),
            ColumnSchema(name="score", type=float),
        ]

    def test_register_and_get_table(self) -> None:
        """测试注册表和获取表"""
        self.registry.register_table("users", self.columns)

        table = self.registry.get_table("users")
        self.assertIsNotNone(table)
        if table is not None:
            self.assertEqual(table.name, "users")
            self.assertEqual(len(table.columns), 4)

    def test_get_nonexistent_table(self) -> None:
        """测试获取不存在的表"""
        table = self.registry.get_table("nonexistent")
        self.assertIsNone(table)

    def test_list_tables(self) -> None:
        """测试列出所有表"""
        self.registry.register_table("users", self.columns)
        self.registry.register_table("orders", [])

        tables = self.registry.list_tables()
        self.assertEqual(len(tables), 2)
        self.assertIn("users", tables)
        self.assertIn("orders", tables)

    def test_list_tables_empty(self) -> None:
        """测试空注册表"""
        tables = self.registry.list_tables()
        self.assertEqual(len(tables), 0)

    def test_get_random_column(self) -> None:
        """测试随机获取列"""
        self.registry.register_table("users", self.columns)

        col = self.registry.get_random_column("users")
        self.assertIsNotNone(col)
        self.assertIn(col, self.columns)

    def test_get_random_column_nonexistent_table(self) -> None:
        """测试从不存在的表获取列"""
        col = self.registry.get_random_column("nonexistent")
        self.assertIsNone(col)

    def test_get_random_column_empty_table(self) -> None:
        """测试从空表获取列"""
        self.registry.register_table("empty", [])
        col = self.registry.get_random_column("empty")
        self.assertIsNone(col)

    def test_get_column(self) -> None:
        """测试获取指定列"""
        self.registry.register_table("users", self.columns)

        col = self.registry.get_column("users", "name")
        self.assertIsNotNone(col)
        if col is not None:
            self.assertEqual(col.name, "name")
            self.assertEqual(col.type, str)

    def test_get_column_nonexistent(self) -> None:
        """测试获取不存在的列"""
        self.registry.register_table("users", self.columns)

        col = self.registry.get_column("users", "nonexistent")
        self.assertIsNone(col)

    def test_generate_value_int(self) -> None:
        """测试为int类型列生成值"""
        col = ColumnSchema(name="age", type=int)
        value = self.registry.generate_value(col)
        self.assertIsInstance(value, int)
        self.assertGreaterEqual(value, 1)
        self.assertLessEqual(value, 1000)

    def test_generate_value_str(self) -> None:
        """测试为str类型列生成值"""
        col = ColumnSchema(name="name", type=str)
        value = self.registry.generate_value(col)
        self.assertIsInstance(value, str)
        self.assertGreaterEqual(len(value), 5)
        self.assertLessEqual(len(value), 20)

    def test_generate_value_float(self) -> None:
        """测试为float类型列生成值"""
        col = ColumnSchema(name="score", type=float)
        value = self.registry.generate_value(col)
        self.assertIsInstance(value, float)

    def test_generate_value_bool(self) -> None:
        """测试为bool类型列生成值"""
        col = ColumnSchema(name="active", type=bool)
        value = self.registry.generate_value(col)
        self.assertIsInstance(value, bool)

    def test_register_table_overwrite(self) -> None:
        """测试覆盖已存在的表"""
        self.registry.register_table("users", self.columns)

        new_columns = [ColumnSchema(name="id", type=int)]
        self.registry.register_table("users", new_columns)

        table = self.registry.get_table("users")
        if table is not None:
            self.assertEqual(len(table.columns), 1)

    def test_register_table_invalid_name(self) -> None:
        """测试注册无效表名"""
        with self.assertRaises(Exception):
            self.registry.register_table("", [])

    def test_register_table_invalid_columns(self) -> None:
        """测试注册无效的列定义"""
        with self.assertRaises(Exception):
            self.registry.register_table("test", "not a list")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
